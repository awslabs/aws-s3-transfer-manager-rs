/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Adaptive concurrency controller.
//!
//! Three-state machine that discovers and maintains the optimal concurrency
//! for the current network conditions.
//!
//! ## Throughput Measurement
//!
//! Throughput comes from shared [`IOCounters`] sliding windows. Raw measurements
//! are smoothed through a windowed max filter (inspired by BBR's bandwidth
//! estimation) before reaching the algorithm. A single low measurement — common
//! at transfer boundaries — doesn't affect the max. Sustained throughput changes
//! are tracked as old samples age out of the window.
//!
//! ## Phases
//!
//! **SlowStart**: Additive growth (+`max_growth` per evaluation). Compares
//! against the best recent measurement; requires consecutive rejections before
//! exiting to ride through noisy warmup windows.
//!
//! **StableProbing**: Small upward probes (+2%). Must beat the best recent
//! measurement by 2%. Three consecutive rejections trigger StableShedding.
//!
//! **StableShedding**: Accelerating downward probes (-2%, -4%, -6%...).
//! Accepts while throughput stays within 2% of peak. Rejects back to
//! StableProbing when throughput drops too far.
//!
//! ## Error-Aware Transitions
//!
//! Throttle errors (503, 429, SlowDown) bypass the evaluation timer and
//! transition immediately to StableShedding.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{CompletionSample, ConcurrencyController, ErrorKind};
use crate::metrics::IOCounters;

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
    /// Evaluation interval. The controller checks throughput this often.
    pub duration: Duration,
    /// Number of raw measurements to retain for windowed-max smoothing.
    /// The algorithm sees the max of the last N measurements, filtering
    /// transient drops (transfer boundary gaps, warmup noise).
    pub smoothing_window: usize,
}

/// SlowStart phase settings.
#[derive(Debug, Clone)]
pub(crate) struct SlowStartConfig {
    /// Evaluation interval during SlowStart. Shorter than the stable phase
    /// interval to ramp concurrency faster while connections are warming up.
    pub evaluation_interval: Duration,
    /// Throughput must improve by this ratio to accept a growth step.
    pub acceptance_margin: f64,
    /// Additive growth per accepted window.
    pub max_growth: usize,
    /// Consecutive rejections required before exiting SlowStart.
    pub exit_threshold: usize,
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
                // 4 measurements = 2s. Transient drops (transfer boundary gaps)
                // don't affect the max; sustained drops age out naturally.
                smoothing_window: 4,
            },
            slow_start: SlowStartConfig {
                // 300ms evaluation during SlowStart for faster ramp.
                evaluation_interval: Duration::from_millis(300),
                // 10% improvement required to accept growth step.
                acceptance_margin: 0.10,
                // +32 connections per accepted window.
                max_growth: 32,
                // Tolerate 3 noisy windows before exiting SlowStart.
                // Connection warmup (TLS handshakes, TCP slow start) causes
                // throughput to lag behind concurrency increases.
                exit_threshold: 4,
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
    /// Smoothed goodput fed to the algorithm (bytes/sec). Each entry is the
    /// windowed max of raw measurements at that evaluation point.
    recent_goodput: VecDeque<f64>,
    /// Raw goodput measurements for windowed-max smoothing. Transient drops
    /// don't affect the max; sustained drops age out naturally.
    raw_goodput: VecDeque<f64>,
    /// Consecutive probe results in the same direction within the current phase.
    /// SlowStart: consecutive rejections (triggers exit at threshold).
    /// StableProbing: consecutive rejections (triggers shedding at threshold).
    /// StableShedding: consecutive accepts (accelerates reduction).
    consecutive_probes: usize,
    last_evaluation: Instant,
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
    /// Shared sliding-window I/O counters. The scheduler records samples;
    /// the controller reads throughput at evaluation boundaries.
    io_counters: Arc<IOCounters>,
    /// Algorithm phase. Stored as AtomicUsize for lock-free reads.
    phase: AtomicUsize,
    /// Algorithm state. Only accessed at window boundaries.
    state: Mutex<AlgorithmState>,
}

impl AdaptiveConcurrencyController {
    pub(crate) fn new(config: AdaptiveConfig, io_counters: Arc<IOCounters>) -> Self {
        let initial = config.initial_concurrency;
        Self {
            target: AtomicUsize::new(initial),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            io_counters,
            phase: AtomicUsize::new(Phase::SlowStart as usize),
            state: Mutex::new(AlgorithmState {
                accepted_concurrency: initial,
                probing_concurrency: initial,
                recent_goodput: VecDeque::with_capacity(config.stable.history_size),
                raw_goodput: VecDeque::with_capacity(config.window.smoothing_window),
                consecutive_probes: 0,
                last_evaluation: Instant::now(),
            }),
            config,
        }
    }

    /// Check if the evaluation interval has elapsed and run the algorithm.
    /// Uses try_lock to avoid blocking workers.
    ///
    /// Returns early (no-op) if:
    /// - Another thread holds the lock (contention avoidance)
    /// - The evaluation interval hasn't elapsed yet
    /// - IOCounters reports idle (no data in the sliding window)
    fn maybe_update_window(&self) {
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        let phase = Phase::from_usize(self.phase.load(Ordering::Relaxed));
        let interval = match phase {
            Phase::SlowStart => self.config.slow_start.evaluation_interval,
            _ => self.config.window.duration,
        };
        if state.last_evaluation.elapsed() < interval {
            return;
        }
        state.last_evaluation = Instant::now();
        let peak = self.peak_in_flight.swap(0, Ordering::Relaxed);

        if self.io_counters.is_idle() {
            return;
        }

        let goodput = self.io_counters.network_throughput();

        // Windowed max: push raw measurement, feed max to algorithm.
        // Transient drops (transfer boundary gaps) don't affect the max.
        state.raw_goodput.push_back(goodput);
        if state.raw_goodput.len() > self.config.window.smoothing_window {
            state.raw_goodput.pop_front();
        }
        let smoothed = state.raw_goodput.iter().copied().fold(0.0f64, f64::max);

        tracing::trace!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            goodput_mbps = goodput / 1_000_000.0,
            smoothed_mbps = smoothed / 1_000_000.0,
            peak,
            "window evaluated"
        );

        let new_target = self.run_algorithm(&mut state, smoothed, peak);
        let clamped = match self.config.max_concurrency {
            Some(max) => new_target.min(max),
            None => new_target,
        }
        .max(1);
        let old_target = self.target.swap(clamped, Ordering::Relaxed);

        let phase = Phase::from_usize(self.phase.load(Ordering::Relaxed));
        let gbps = crate::metrics::unit::ByteUnit::Gigabit.as_bytes_u64() as f64;
        tracing::trace!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            goodput_gbps = goodput / gbps,
            smoothed_gbps = smoothed / gbps,
            accepted = state.accepted_concurrency,
            target = clamped,
            ?phase,
            peak,
            "evaluation"
        );
        if old_target != clamped {
            tracing::debug!(
                target: crate::telemetry::TARGET_CONCURRENCY,
                old_target,
                new_target = clamped,
                accepted = state.accepted_concurrency,
                ?phase,
                "concurrency target updated"
            );
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
            state.raw_goodput.clear();
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
    /// improved by >= acceptance_margin over the best recent measurement.
    /// Requires multiple consecutive rejections before exiting to tolerate
    /// noisy measurements during connection warmup.
    fn run_slow_start(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let best = state
            .recent_goodput
            .iter()
            .rev()
            .skip(1)
            .copied()
            .fold(0.0f64, f64::max);
        let ratio = ratio(measured, best);
        let accept = ratio >= 1.0 + self.config.slow_start.acceptance_margin;

        if accept {
            state.consecutive_probes = 0;
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::SlowStart, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, "probe accepted");
            state.accepted_concurrency = state.probing_concurrency;
            state.probing_concurrency =
                state.accepted_concurrency + self.config.slow_start.max_growth;
        } else {
            state.consecutive_probes += 1;
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::SlowStart, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, consecutive = state.consecutive_probes, threshold = self.config.slow_start.acceptance_margin, "SlowStart plateau");
            if state.consecutive_probes >= self.config.slow_start.exit_threshold {
                self.transition_to(state, Phase::StableProbing);
                state.probing_concurrency = (state.accepted_concurrency as f64
                    * (1.0 + self.config.stable.probe_pct))
                    .ceil() as usize;
            }
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
    use super::super::{CompletionSample, ErrorKind};
    use super::*;
    use crate::metrics::{IOCounters, IoSample};
    use crate::transfer::IoKind;
    use std::sync::Arc;

    struct TestController(AdaptiveConcurrencyController);

    impl TestController {
        fn new(config: AdaptiveConfig) -> Self {
            let io_counters = Arc::new(IOCounters::new(config.window.duration));
            Self(AdaptiveConcurrencyController::new(config, io_counters))
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

        /// Drive through SlowStart to StableProbing by feeding flat throughput.
        /// Returns with accepted = initial_concurrency, history cleared.
        fn exit_slow_start(&self) {
            self.record_goodput(1000.0); // bootstrap
                                         // Need exit_threshold consecutive rejections (default 3)
            let threshold = self.0.config.slow_start.exit_threshold;
            for _ in 0..threshold {
                self.record_goodput(1000.0);
            }
            assert_eq!(self.phase(), Phase::StableProbing);
        }
    }

    fn test_config() -> AdaptiveConfig {
        AdaptiveConfig::default()
    }

    fn sample(bytes_sent: u64) -> CompletionSample {
        CompletionSample {
            io: IoSample {
                network_tx: bytes_sent,
                ..Default::default()
            },
            error: None,
            kind: IoKind::Network,
        }
    }

    // -- Bootstrap --

    #[test]
    fn bootstrap_records_first_measurement_without_running_algorithm() {
        let tc = TestController::new(test_config());
        let target = tc.record_goodput(1000.0);
        // First measurement: bootstrap sets probing = 32 + 32 = 64
        assert_eq!(tc.phase(), Phase::SlowStart);
        assert_eq!(target, 64);
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
        assert_eq!(tc.accepted(), 64); // accepted = previous probing (32+32)
    }

    #[test]
    fn slow_start_rejects_insufficient_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1050.0); // 5% < 10% threshold, rejection 1
        assert_eq!(tc.phase(), Phase::SlowStart); // still in SlowStart
        tc.record_goodput(1050.0); // rejection 2
        assert_eq!(tc.phase(), Phase::SlowStart);
        tc.record_goodput(1050.0); // rejection 3
        assert_eq!(tc.phase(), Phase::SlowStart);
        tc.record_goodput(1050.0); // rejection 4 -> StableProbing
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 32);
    }

    // -- StableProbing --

    #[test]
    fn stable_probing_accepts_on_improvement() {
        let tc = TestController::new(test_config());
        tc.exit_slow_start();
        tc.record_goodput(1000.0); // StableProbing bootstrap
        tc.record_goodput(1030.0); // probing=33, 1030/1000=1.03 > 1.02, accept
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 33); // accepted the probe
        assert_eq!(tc.consecutive_probes(), 0); // reset on accept
    }

    #[test]
    fn stable_probing_rejects_to_shedding_after_threshold() {
        let tc = TestController::new(test_config());
        tc.exit_slow_start();
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
        tc.exit_slow_start();
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
        tc.exit_slow_start();
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
                duration: Duration::from_nanos(10),
                smoothing_window: 4,
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
                duration: Duration::from_nanos(10),
                smoothing_window: 4,
            },
            ..Default::default()
        };
        let tc = TestController::new(config);

        // Feed two windows of increasing goodput through on_completion
        // to trigger SlowStart accept and target update.
        for _ in 0..20 {
            tc.0.io_counters.record(&IoSample {
                network_tx: 8_000_000,
                ..Default::default()
            });
            tc.0.on_completion(&sample(8_000_000));
        }
        // Force window boundary by sleeping briefly
        std::thread::sleep(Duration::from_millis(1));
        for _ in 0..20 {
            tc.0.io_counters.record(&IoSample {
                network_tx: 16_000_000,
                ..Default::default()
            });
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
        tc.exit_slow_start();
        tc.record_goodput(1000.0); // StableProbing bootstrap, accepted=32
        let target = tc.record_goodput_with_peak(5000.0, 5); // peak=5, below accepted=32
        assert_eq!(tc.accepted(), 32); // not accepted, probe not exercised
        assert!(target >= 32); // still probing
    }

    #[test]
    fn exercised_check_passes_when_exercised() {
        // In StableProbing, peak in-flight matches accepted, throughput improved.
        let tc = TestController::new(test_config());
        tc.exit_slow_start();
        tc.record_goodput(1000.0); // StableProbing bootstrap, accepted=32
        tc.record_goodput_with_peak(1030.0, 32); // peak=32, 3% > 2%
        assert_eq!(tc.accepted(), 33); // accepted the probe
    }

    // -- SlowStart additive growth --

    #[test]
    fn slow_start_additive_growth() {
        let tc = TestController::new(test_config());
        // Bootstrap: first measurement, probing = 32 + 32 = 64
        let target = tc.record_goodput(1000.0);
        assert_eq!(target, 64);
        assert_eq!(tc.accepted(), 32);
        // Accept: throughput improved 20%, accepted = 64, probing = 64 + 32 = 96
        let target = tc.record_goodput(1200.0);
        assert_eq!(tc.accepted(), 64);
        assert_eq!(target, 96);
    }

    #[test]
    fn slow_start_exits_on_plateau() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1050.0); // rejection 1
        tc.record_goodput(1050.0); // rejection 2
        tc.record_goodput(1050.0); // rejection 3
        tc.record_goodput(1050.0); // rejection 4 -> StableProbing
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 32);
    }

    // -- Gap filtering --

    #[test]
    fn idle_window_skips_evaluation() {
        // When IOCounters reports idle (no completions in the window),
        // the controller should not evaluate or record history.
        let config = AdaptiveConfig {
            window: WindowConfig {
                duration: Duration::from_nanos(10),
                smoothing_window: 4,
            },
            ..Default::default()
        };
        let io_counters = Arc::new(IOCounters::new(Duration::from_secs(1)));
        let controller = AdaptiveConcurrencyController::new(config, Arc::clone(&io_counters));
        // Don't record any bytes — IOCounters is idle
        controller.on_dispatch();
        controller.on_completion(&sample(0));
        // History should be empty — idle window was skipped
        let state = controller.state.lock().unwrap();
        assert_eq!(state.recent_goodput.len(), 0, "idle window was evaluated");
    }

    // -- History management --

    #[test]
    fn history_cleared_on_slow_start_exit() {
        let tc = TestController::new(test_config());
        tc.exit_slow_start();
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.history_len(), 0); // clean slate for stable phase
    }

    #[test]
    fn history_preserved_between_stable_phases() {
        let tc = TestController::new(test_config());
        tc.exit_slow_start();

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
        tc.exit_slow_start();

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
        tc.exit_slow_start();

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
