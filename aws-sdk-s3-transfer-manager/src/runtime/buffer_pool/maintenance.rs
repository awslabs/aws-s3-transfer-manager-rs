/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Idle reclamation policy and maintenance-worker coordination.
//!
//! [`MaintenanceState`] converts explicit activity, deadline, and completion
//! events into serialized maintenance actions. Pool and platform operations
//! execute outside this state machine. The worker retains only control state
//! and a weak pool reference while waiting.

use std::time::{Duration, Instant};

use super::admission::AdmissionGuard;
use super::arena::ArenaTrim;
use super::{CarrierCount, PoolInner};
use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering as DiagnosticOrdering};
use crate::runtime::sync::sync::{Arc, Condvar, Mutex};

#[cfg(not(all(test, s3_tm_loom)))]
use std::{io, sync::Weak};

#[cfg(not(all(test, s3_tm_loom)))]
use crate::runtime::sync::thread;

/// Fraction of the configured block ceiling retained after an idle deadline.
const IDLE_RETENTION_DIVISOR: usize = 4;

/// Initial cache-hysteresis interval after scheduler-global idle.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay before retrying blocked reclamation or mapping cleanup.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Operating-system thread name used by the pool maintenance worker.
const WORKER_NAME: &str = "s3-tm-memory";

/// One maintenance operation authorized by the control state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MaintenanceAction {
    /// Retry pending mapping protection or backing discard.
    RetryCleanup {
        /// Cleanup generation observed when the action was issued.
        generation: u64,
    },
    /// Reclaim free blocks down to one fixed target.
    Reclaim {
        /// Activity epoch that authorized this reclaim pass.
        epoch: u64,
        /// Prepared capacity retained for the complete idle epoch.
        target: CarrierCount,
    },
}

/// Result of one bounded maintenance pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MaintenanceOutcome {
    /// No eligible work remains for this request.
    Complete,
    /// Eligible work may appear or recover after a bounded delay.
    Retry,
}

/// Result of executing one policy action against pool state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MaintenancePass {
    /// Whether the action needs another bounded attempt.
    pub(super) outcome: MaintenanceOutcome,
    /// Whether mapping cleanup must run independently of reclamation.
    pub(super) cleanup_pending: bool,
}

impl MaintenancePass {
    /// Constructs a pass result without cleanup recovery.
    fn new(outcome: MaintenanceOutcome) -> Self {
        Self {
            outcome,
            cleanup_pending: false,
        }
    }
}

/// Lazy worker owner embedded in the pool lifetime domain.
pub(super) struct MaintenanceCoordinator {
    /// Immutable normal admission ceiling copied at pool construction.
    configured_capacity: CarrierCount,
    /// Immutable whole-block carrier count copied from pool geometry.
    block_capacity: CarrierCount,
    /// State and wakeup primitive retained independently by the worker.
    control: Arc<MaintenanceControl>,
    /// Counters that observe worker behavior without controlling it.
    diagnostics: Arc<MaintenanceDiagnostics>,
    /// Serialized worker creation and final join authority.
    #[cfg(not(all(test, s3_tm_loom)))]
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MaintenanceCoordinator {
    /// Creates an idle coordinator without starting a thread.
    pub(super) fn new(configured_capacity: CarrierCount, block_capacity: CarrierCount) -> Self {
        Self {
            configured_capacity,
            block_capacity,
            control: Arc::new(MaintenanceControl::new()),
            diagnostics: Arc::new(MaintenanceDiagnostics::default()),
            #[cfg(not(all(test, s3_tm_loom)))]
            worker: Mutex::new(None),
        }
    }

    /// Invalidates a pending idle epoch without starting the worker.
    pub(super) fn record_activity(&self) {
        self.control.state.lock().record_activity();
        self.control.wake.notify_all();
    }

    /// Arms reclamation after scheduler-global idle.
    pub(super) fn record_idle(&self, pool: &Arc<PoolInner>) {
        self.record_idle_after(pool, Instant::now(), IDLE_TIMEOUT);
    }

    /// Requests recovery for a failed mapping cleanup operation.
    pub(super) fn request_cleanup(&self, pool: &Arc<PoolInner>) {
        if self.control.state.lock().request_cleanup(Instant::now()) {
            self.diagnostics.record_cleanup_request();
        }
        self.ensure_worker(pool);
        self.control.wake.notify_all();
    }

    /// Starts the worker at most once while maintenance remains enabled.
    #[cfg(not(all(test, s3_tm_loom)))]
    fn ensure_worker(&self, pool: &Arc<PoolInner>) {
        let mut worker = self.worker.lock();
        if worker.is_some() {
            return;
        }
        {
            let state = self.control.state.lock();
            if state.is_stopping() || state.is_disabled() {
                return;
            }
        }

        let configured_capacity = self.configured_capacity;
        let block_capacity = self.block_capacity;
        let control = Arc::clone(&self.control);
        let diagnostics = Arc::clone(&self.diagnostics);
        let weak_pool = Arc::downgrade(pool);

        #[cfg(test)]
        pool.test_hooks.record_maintenance_spawn_attempt();
        let spawned = if maintenance_spawn_failure_injected(pool) {
            Err(io::Error::other("injected maintenance worker failure"))
        } else {
            thread::Builder::new()
                .name(WORKER_NAME.to_owned())
                .spawn(move || {
                    worker_loop(
                        control,
                        diagnostics,
                        weak_pool,
                        configured_capacity,
                        block_capacity,
                    )
                })
        };

        match spawned {
            Ok(handle) => {
                self.diagnostics.record_worker_start();
                tracing::debug!(
                    target: crate::telemetry::TARGET_MEMORY,
                    worker = WORKER_NAME,
                    "started buffer-pool maintenance worker"
                );
                *worker = Some(handle);
            }
            Err(error) => {
                self.diagnostics.record_worker_start_failure();
                tracing::warn!(
                    target: crate::telemetry::TARGET_MEMORY,
                    worker = WORKER_NAME,
                    error = %error,
                    "buffer-pool maintenance is disabled after worker creation failed"
                );
                self.control.state.lock().disable();
            }
        }
    }

    /// Leaves wall-clock execution outside the Loom state model.
    #[cfg(all(test, s3_tm_loom))]
    fn ensure_worker(&self, _pool: &Arc<PoolInner>) {}

    /// Cancels maintenance and joins a worker owned by another thread.
    pub(super) fn shutdown(&mut self) {
        self.control.state.lock().stop();
        self.control.wake.notify_all();

        #[cfg(not(all(test, s3_tm_loom)))]
        let Some(worker) = self.worker.lock().take() else {
            return;
        };
        #[cfg(not(all(test, s3_tm_loom)))]
        if worker.thread().id() == thread::current().id() {
            return;
        }
        #[cfg(not(all(test, s3_tm_loom)))]
        if worker.join().is_err() {
            tracing::warn!(
                target: crate::telemetry::TARGET_MEMORY,
                worker = WORKER_NAME,
                "buffer-pool maintenance worker terminated unexpectedly"
            );
        }
    }

    /// Arms an idle deadline from caller-supplied clock input.
    fn record_idle_after(&self, pool: &Arc<PoolInner>, now: Instant, timeout: Duration) {
        if self.control.state.lock().record_idle(now, timeout) {
            self.diagnostics.record_idle_deadline();
            tracing::debug!(
                target: crate::telemetry::TARGET_MEMORY,
                timeout_millis = timeout.as_millis(),
                "armed buffer-pool idle reclamation"
            );
        }
        self.ensure_worker(pool);
        self.control.wake.notify_all();
    }

    /// Returns one relaxed operational diagnostic sample.
    pub(super) fn diagnostics(&self) -> MaintenanceDiagnosticSnapshot {
        self.diagnostics.snapshot()
    }

    /// Returns whether a worker handle has been published.
    #[cfg(test)]
    fn worker_started(&self) -> bool {
        #[cfg(not(s3_tm_loom))]
        {
            self.worker.lock().is_some()
        }
        #[cfg(s3_tm_loom)]
        {
            false
        }
    }

    /// Returns the published worker name.
    #[cfg(all(test, not(s3_tm_loom)))]
    fn worker_name(&self) -> Option<String> {
        self.worker
            .lock()
            .as_ref()
            .and_then(|worker| worker.thread().name().map(str::to_owned))
    }

    /// Returns whether thread creation permanently disabled maintenance.
    #[cfg(test)]
    fn is_disabled(&self) -> bool {
        self.control.state.lock().is_disabled()
    }
}

/// Wait state retained by the worker without retaining pool storage.
struct MaintenanceControl {
    /// Serialized policy state.
    state: Mutex<MaintenanceState>,
    /// Deadline, request, and shutdown wakeups.
    wake: Condvar,
}

impl MaintenanceControl {
    /// Creates control state with no pending work.
    fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceState::new()),
            wake: Condvar::new(),
        }
    }
}

/// Counters that never participate in maintenance decisions.
#[derive(Default)]
struct MaintenanceDiagnostics {
    /// Scheduler-global idle intervals that armed a new deadline.
    idle_deadlines: AtomicU64,
    /// Successfully created maintenance workers.
    worker_starts: AtomicU64,
    /// Worker creation failures that disabled maintenance.
    worker_start_failures: AtomicU64,
    /// Reclaim actions executed by the worker.
    reclaim_passes: AtomicU64,
    /// Reclaim actions that remained blocked after a bounded pass.
    reclaim_retries: AtomicU64,
    /// Cleanup generations requested after a mapping failure.
    cleanup_requests: AtomicU64,
}

impl MaintenanceDiagnostics {
    /// Records one newly armed idle deadline.
    fn record_idle_deadline(&self) {
        saturating_add(&self.idle_deadlines, 1);
    }

    /// Records successful worker creation.
    fn record_worker_start(&self) {
        saturating_add(&self.worker_starts, 1);
    }

    /// Records a worker creation failure.
    fn record_worker_start_failure(&self) {
        saturating_add(&self.worker_start_failures, 1);
    }

    /// Records one bounded reclaim action and whether it needs retry.
    fn record_reclaim_pass(&self, outcome: MaintenanceOutcome) {
        saturating_add(&self.reclaim_passes, 1);
        if outcome == MaintenanceOutcome::Retry {
            saturating_add(&self.reclaim_retries, 1);
        }
    }

    /// Records a new cleanup request generation.
    fn record_cleanup_request(&self) {
        saturating_add(&self.cleanup_requests, 1);
    }

    /// Loads one relaxed diagnostic sample.
    fn snapshot(&self) -> MaintenanceDiagnosticSnapshot {
        MaintenanceDiagnosticSnapshot {
            idle_deadlines: self.idle_deadlines.load(DiagnosticOrdering::Relaxed),
            worker_starts: self.worker_starts.load(DiagnosticOrdering::Relaxed),
            worker_start_failures: self.worker_start_failures.load(DiagnosticOrdering::Relaxed),
            reclaim_passes: self.reclaim_passes.load(DiagnosticOrdering::Relaxed),
            reclaim_retries: self.reclaim_retries.load(DiagnosticOrdering::Relaxed),
            cleanup_requests: self.cleanup_requests.load(DiagnosticOrdering::Relaxed),
        }
    }
}

/// Private snapshot of maintenance scheduling and worker behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MaintenanceDiagnosticSnapshot {
    /// Scheduler-global idle intervals that armed a new deadline.
    pub(super) idle_deadlines: u64,
    /// Successfully created maintenance workers.
    pub(super) worker_starts: u64,
    /// Worker creation failures that disabled maintenance.
    pub(super) worker_start_failures: u64,
    /// Reclaim actions executed by the worker.
    pub(super) reclaim_passes: u64,
    /// Reclaim actions that remained blocked after a bounded pass.
    pub(super) reclaim_retries: u64,
    /// Cleanup generations requested after a mapping failure.
    pub(super) cleanup_requests: u64,
}

/// Deadline armed for one scheduler-idle interval.
#[derive(Clone, Copy, Debug)]
struct IdleDeadline {
    /// Activity epoch current when global idle was observed.
    epoch: u64,
    /// Earliest time at which reclamation may start.
    expires_at: Instant,
}

/// Reclaim request whose target remains fixed across retries.
#[derive(Clone, Copy, Debug)]
struct ReclaimRequest {
    /// Activity epoch current when the idle deadline expired.
    epoch: u64,
    /// Whole-block capacity retained by this request.
    target: CarrierCount,
    /// Earliest time at which another pass may run.
    eligible_at: Instant,
}

/// Cleanup request protected against completion of an older pass.
#[derive(Clone, Copy, Debug)]
struct CleanupRequest {
    /// Monotonic request generation.
    generation: u64,
    /// Earliest time at which another pass may run.
    eligible_at: Instant,
}

/// Serialized idle, retry, disable, and stop policy.
pub(super) struct MaintenanceState {
    /// Generation changed by every managed-activity transition.
    activity_epoch: u64,
    /// Pending scheduler-idle deadline.
    idle_deadline: Option<IdleDeadline>,
    /// Pending reclaim request with a stable retention target.
    reclaim: Option<ReclaimRequest>,
    /// Latest mapping-cleanup request.
    cleanup: Option<CleanupRequest>,
    /// Generation assigned to the next cleanup request.
    next_cleanup_generation: u64,
    /// Final destruction has requested worker termination.
    stopping: bool,
    /// Thread creation failed and maintenance is permanently disabled.
    disabled: bool,
}

impl MaintenanceState {
    /// Creates enabled maintenance state with no pending work.
    pub(super) fn new() -> Self {
        Self {
            activity_epoch: 0,
            idle_deadline: None,
            reclaim: None,
            cleanup: None,
            next_cleanup_generation: 1,
            stopping: false,
            disabled: false,
        }
    }

    /// Invalidates idle reclamation after new managed work begins.
    ///
    /// Cleanup recovery is independent of scheduler activity and remains
    /// pending.
    pub(super) fn record_activity(&mut self) {
        self.activity_epoch = self.activity_epoch.wrapping_add(1);
        self.idle_deadline = None;
        self.reclaim = None;
    }

    /// Arms one deadline for the current global-idle interval.
    ///
    /// Repeated idle observations in the same activity epoch preserve the
    /// original deadline and cannot extend cache retention. Returns `true`
    /// when this call arms a new deadline.
    pub(super) fn record_idle(&mut self, now: Instant, idle_timeout: Duration) -> bool {
        if self.stopping || self.disabled || self.reclaim.is_some() {
            return false;
        }
        if self
            .idle_deadline
            .is_some_and(|deadline| deadline.epoch == self.activity_epoch)
        {
            return false;
        }
        let expires_at = now
            .checked_add(idle_timeout)
            .unwrap_or_else(|| super::invariant_violation("maintenance idle deadline overflow"));
        self.idle_deadline = Some(IdleDeadline {
            epoch: self.activity_epoch,
            expires_at,
        });
        true
    }

    /// Requests mapping cleanup without losing a concurrent newer request.
    /// Returns `true` when this call publishes a new cleanup generation.
    pub(super) fn request_cleanup(&mut self, now: Instant) -> bool {
        if self.stopping || self.disabled {
            return false;
        }
        let generation = self.next_cleanup_generation;
        self.next_cleanup_generation = self.next_cleanup_generation.wrapping_add(1);
        self.cleanup = Some(CleanupRequest {
            generation,
            eligible_at: now,
        });
        true
    }

    /// Returns one due action without consuming its request.
    ///
    /// Cleanup has priority because a pending protection recovery can keep a
    /// complete block unavailable to ordinary allocation.
    pub(super) fn next_action(
        &mut self,
        now: Instant,
        configured_capacity: CarrierCount,
        block_capacity: CarrierCount,
    ) -> Option<MaintenanceAction> {
        if self.stopping || self.disabled {
            return None;
        }

        if let Some(cleanup) = self.cleanup {
            if cleanup.eligible_at <= now {
                return Some(MaintenanceAction::RetryCleanup {
                    generation: cleanup.generation,
                });
            }
        }

        if let Some(deadline) = self.idle_deadline {
            if deadline.expires_at <= now {
                self.idle_deadline = None;
                if deadline.epoch == self.activity_epoch {
                    self.reclaim = Some(ReclaimRequest {
                        epoch: deadline.epoch,
                        target: idle_retention_target(configured_capacity, block_capacity),
                        eligible_at: now,
                    });
                }
            }
        }

        self.reclaim
            .filter(|request| request.eligible_at <= now)
            .map(|request| MaintenanceAction::Reclaim {
                epoch: request.epoch,
                target: request.target,
            })
    }

    /// Records an action result and schedules bounded retry when required.
    ///
    /// Completion from an obsolete action leaves newer work unchanged.
    pub(super) fn finish_action(
        &mut self,
        action: MaintenanceAction,
        outcome: MaintenanceOutcome,
        now: Instant,
        retry_delay: Duration,
    ) {
        let eligible_at = now
            .checked_add(retry_delay)
            .unwrap_or_else(|| super::invariant_violation("maintenance retry deadline overflow"));
        match action {
            MaintenanceAction::RetryCleanup { generation } => {
                let Some(request) = self.cleanup.as_mut() else {
                    return;
                };
                if request.generation != generation {
                    return;
                }
                match outcome {
                    MaintenanceOutcome::Complete => self.cleanup = None,
                    MaintenanceOutcome::Retry => request.eligible_at = eligible_at,
                }
            }
            MaintenanceAction::Reclaim { epoch, target } => {
                let Some(request) = self.reclaim.as_mut() else {
                    return;
                };
                if request.epoch != epoch || request.target != target {
                    return;
                }
                match outcome {
                    MaintenanceOutcome::Complete => self.reclaim = None,
                    MaintenanceOutcome::Retry => request.eligible_at = eligible_at,
                }
            }
        }
    }

    /// Permanently disables maintenance after worker creation fails.
    pub(super) fn disable(&mut self) {
        self.disabled = true;
        self.idle_deadline = None;
        self.reclaim = None;
        self.cleanup = None;
    }

    /// Cancels pending work and requests worker termination.
    pub(super) fn stop(&mut self) {
        self.stopping = true;
        self.idle_deadline = None;
        self.reclaim = None;
        self.cleanup = None;
    }

    /// Returns whether final destruction requested termination.
    pub(super) fn is_stopping(&self) -> bool {
        self.stopping
    }

    /// Returns whether worker creation permanently disabled maintenance.
    pub(super) fn is_disabled(&self) -> bool {
        self.disabled
    }

    /// Returns the earliest time at which pending work can change state.
    pub(super) fn next_wake(&self) -> Option<Instant> {
        [
            self.cleanup.map(|request| request.eligible_at),
            self.reclaim.map(|request| request.eligible_at),
            self.idle_deadline.map(|deadline| deadline.expires_at),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

/// Runs due maintenance while retaining only control state between passes.
#[cfg(not(all(test, s3_tm_loom)))]
fn worker_loop(
    control: Arc<MaintenanceControl>,
    diagnostics: Arc<MaintenanceDiagnostics>,
    pool: Weak<PoolInner>,
    configured_capacity: CarrierCount,
    block_capacity: CarrierCount,
) {
    loop {
        let Some(action) = wait_for_action(&control, configured_capacity, block_capacity) else {
            return;
        };
        let Some(pool) = pool.upgrade() else {
            return;
        };

        #[cfg(test)]
        pool.test_hooks.wait_after_maintenance_upgrade();
        let pass = pool.execute_maintenance(action);
        drop(pool);

        if matches!(action, MaintenanceAction::Reclaim { .. }) {
            diagnostics.record_reclaim_pass(pass.outcome);
        }

        let now = Instant::now();
        let mut state = control.state.lock();
        if pass.cleanup_pending && state.request_cleanup(now) {
            diagnostics.record_cleanup_request();
        }
        state.finish_action(action, pass.outcome, now, RETRY_DELAY);
        let stopping = state.is_stopping();
        drop(state);
        control.wake.notify_all();
        if stopping {
            return;
        }
    }
}

/// Adds a diagnostic value without wrapping.
fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(
        DiagnosticOrdering::Relaxed,
        DiagnosticOrdering::Relaxed,
        |current| Some(current.saturating_add(value)),
    );
}

/// Waits until one action is due or final destruction requests stop.
#[cfg(not(all(test, s3_tm_loom)))]
fn wait_for_action(
    control: &MaintenanceControl,
    configured_capacity: CarrierCount,
    block_capacity: CarrierCount,
) -> Option<MaintenanceAction> {
    let mut state = control.state.lock();
    loop {
        if state.is_stopping() {
            return None;
        }

        let now = Instant::now();
        if let Some(action) = state.next_action(now, configured_capacity, block_capacity) {
            return Some(action);
        }

        state = match state.next_wake() {
            Some(deadline) => {
                let timeout = deadline.saturating_duration_since(now);
                if timeout.is_zero() {
                    continue;
                }
                control.wake.wait_timeout(state, timeout).0
            }
            None => control.wake.wait(state),
        };
    }
}

/// Returns whether a test requested one worker-creation failure.
#[cfg(all(test, not(s3_tm_loom)))]
fn maintenance_spawn_failure_injected(pool: &PoolInner) -> bool {
    pool.test_hooks.take_maintenance_spawn_failure()
}

/// Compiles worker failure injection out of production builds.
#[cfg(not(test))]
fn maintenance_spawn_failure_injected(_pool: &PoolInner) -> bool {
    false
}

impl PoolInner {
    /// Executes one maintenance action without holding maintenance control.
    pub(super) fn execute_maintenance(&self, action: MaintenanceAction) -> MaintenancePass {
        match action {
            MaintenanceAction::RetryCleanup { .. } => {
                if self.arena.retry_cleanup() {
                    MaintenancePass::new(MaintenanceOutcome::Complete)
                } else {
                    MaintenancePass::new(MaintenanceOutcome::Retry)
                }
            }
            MaintenanceAction::Reclaim { target, .. } => self.reclaim_to(target),
        }
    }

    /// Reclaims complete free blocks until the target or a blocker is reached.
    fn reclaim_to(&self, target: CarrierCount) -> MaintenancePass {
        let mut cleanup_pending = false;
        loop {
            let trim = {
                let mut admission = AdmissionGuard::new(self.admission.lock());
                if admission.prepared_capacity() <= target {
                    return MaintenancePass {
                        outcome: MaintenanceOutcome::Complete,
                        cleanup_pending,
                    };
                }
                let admission_floor = admission
                    .acquisition_floor(&self.coverage)
                    .unwrap_or_else(|_| super::invariant_violation("maintenance floor overflow"));
                let floor = std::cmp::max(target, admission_floor);
                self.arena.start_trim(&mut admission, floor)
            };

            let ArenaTrim::Started(cleanup) = trim else {
                return MaintenancePass {
                    outcome: MaintenanceOutcome::Retry,
                    cleanup_pending,
                };
            };
            match cleanup.finish() {
                Ok(()) => {
                    self.arena.record_block_reclaimed();
                    tracing::debug!(
                        target: crate::telemetry::TARGET_MEMORY,
                        "reclaimed one buffer-pool block"
                    );
                }
                Err(error) => {
                    cleanup_pending = true;
                    tracing::warn!(
                        target: crate::telemetry::TARGET_MEMORY,
                        error = ?error,
                        "buffer-pool block cleanup remains pending"
                    );
                }
            }
        }
    }
}

/// Computes one whole-block retention target for an idle epoch.
fn idle_retention_target(
    configured_capacity: CarrierCount,
    block_capacity: CarrierCount,
) -> CarrierCount {
    if configured_capacity == CarrierCount::ZERO || block_capacity == CarrierCount::ZERO {
        return CarrierCount::ZERO;
    }
    let configured_blocks = configured_capacity.get().div_ceil(block_capacity.get());
    let retained_blocks = configured_blocks.div_ceil(IDLE_RETENTION_DIVISOR);
    CarrierCount::new(
        retained_blocks
            .checked_mul(block_capacity.get())
            .unwrap_or_else(|| super::invariant_violation("idle retention target overflow")),
    )
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::*;
    use crate::runtime::buffer_pool::test_util::test_pool;
    use crate::runtime::buffer_pool::virtual_memory::VirtualMemoryOperation;
    use std::sync::Barrier;

    const IDLE: Duration = Duration::from_secs(10);
    const RETRY: Duration = Duration::from_secs(2);

    /// Waits for one worker-owned state transition without choosing its order.
    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(Instant::now() < deadline, "maintenance worker timed out");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn carriers(value: usize) -> CarrierCount {
        CarrierCount::new(value)
    }

    #[test]
    fn test_repeated_idle_does_not_extend_the_deadline() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, IDLE);
        state.record_idle(start + Duration::from_secs(5), IDLE);

        assert_eq!(
            state.next_action(start + IDLE, carriers(256), carriers(64)),
            Some(MaintenanceAction::Reclaim {
                epoch: 0,
                target: carriers(64),
            })
        );
    }

    #[test]
    fn test_activity_invalidates_a_stale_idle_deadline() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, IDLE);
        state.record_activity();

        assert_eq!(
            state.next_action(start + IDLE, carriers(256), carriers(64)),
            None
        );
    }

    #[test]
    fn test_idle_target_rounds_to_complete_blocks() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, IDLE);

        assert_eq!(
            state.next_action(start + IDLE, carriers(70), carriers(64)),
            Some(MaintenanceAction::Reclaim {
                epoch: 0,
                target: carriers(64),
            })
        );
    }

    #[test]
    fn test_reclaim_retry_keeps_the_epoch_target() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, IDLE);
        let action = state
            .next_action(start + IDLE, carriers(1024), carriers(64))
            .unwrap();
        assert_eq!(
            action,
            MaintenanceAction::Reclaim {
                epoch: 0,
                target: carriers(256),
            }
        );
        state.finish_action(action, MaintenanceOutcome::Retry, start + IDLE, RETRY);

        assert_eq!(
            state.next_action(start + IDLE + RETRY, carriers(64), carriers(64)),
            Some(MaintenanceAction::Reclaim {
                epoch: 0,
                target: carriers(256),
            })
        );
    }

    #[test]
    fn test_activity_cancels_a_pending_reclaim_retry() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, IDLE);
        let action = state
            .next_action(start + IDLE, carriers(256), carriers(64))
            .unwrap();
        state.finish_action(action, MaintenanceOutcome::Retry, start + IDLE, RETRY);
        state.record_activity();

        assert_eq!(
            state.next_action(start + IDLE + RETRY, carriers(256), carriers(64)),
            None
        );
    }

    #[test]
    fn test_new_cleanup_request_survives_old_completion() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.request_cleanup(start);
        let old = state.next_action(start, carriers(1), carriers(1)).unwrap();
        state.request_cleanup(start);
        state.finish_action(old, MaintenanceOutcome::Complete, start, RETRY);

        assert!(matches!(
            state.next_action(start, carriers(1), carriers(1)),
            Some(MaintenanceAction::RetryCleanup { generation: 2 })
        ));
    }

    #[test]
    fn test_cleanup_retry_is_delayed_and_has_priority() {
        let start = Instant::now();
        let mut state = MaintenanceState::new();
        state.record_idle(start, Duration::ZERO);
        state.request_cleanup(start);
        let cleanup = state
            .next_action(start, carriers(256), carriers(64))
            .unwrap();
        assert!(matches!(cleanup, MaintenanceAction::RetryCleanup { .. }));
        state.finish_action(cleanup, MaintenanceOutcome::Retry, start, RETRY);

        assert!(matches!(
            state.next_action(start, carriers(256), carriers(64)),
            Some(MaintenanceAction::Reclaim { .. })
        ));
        assert!(matches!(
            state.next_action(start + RETRY, carriers(256), carriers(64)),
            Some(MaintenanceAction::RetryCleanup { .. })
        ));
    }

    #[test]
    fn test_disable_and_stop_cancel_all_work() {
        let start = Instant::now();
        for stop in [false, true] {
            let mut state = MaintenanceState::new();
            state.record_idle(start, Duration::ZERO);
            state.request_cleanup(start);
            if stop {
                state.stop();
                assert!(state.is_stopping());
            } else {
                state.disable();
            }
            assert_eq!(state.next_wake(), None);
            assert_eq!(state.next_action(start, carriers(1), carriers(1)), None);
        }
    }

    #[test]
    fn test_reclaim_trims_free_blocks_to_the_fixed_target() {
        let (pool, carrier_size) = test_pool(2, 8);
        let owned = pool.acquire_unreserved(carrier_size * 8).unwrap();
        drop(owned);

        let pass = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: carriers(2),
        });

        assert_eq!(pass, MaintenancePass::new(MaintenanceOutcome::Complete));
        assert_eq!(
            pool.inner.admission.lock().ledger.prepared_capacity,
            carriers(2)
        );
        assert_eq!(pool.inner.arena.diagnostics().blocks_reclaimed, 3);
    }

    #[test]
    fn test_reclaim_retries_while_live_ownership_blocks_trim() {
        let (pool, carrier_size) = test_pool(2, 2);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();

        let blocked = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(blocked, MaintenancePass::new(MaintenanceOutcome::Retry));

        drop(owned);
        let complete = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(complete, MaintenancePass::new(MaintenanceOutcome::Complete));
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);
    }

    #[test]
    fn test_reclaim_preserves_the_current_admission_floor() {
        let (pool, carrier_size) = test_pool(2, 4);
        let reservation = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("reservation");

        let blocked = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(blocked.outcome, MaintenanceOutcome::Retry);
        assert_eq!(
            pool.metrics().prepared_capacity_bytes(),
            (carrier_size * 4) as u64
        );

        drop(reservation);
        let complete = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(complete.outcome, MaintenanceOutcome::Complete);
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);
    }

    #[test]
    fn test_failed_trim_requests_cleanup_without_restoring_prepared_capacity() {
        let (pool, carrier_size) = test_pool(1, 1);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        drop(owned);
        let slot = pool
            .inner
            .arena
            .select_trim_candidate()
            .expect("free prepared slot");
        slot.inject_failure_once(VirtualMemoryOperation::Deactivate);

        let trim = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(trim.outcome, MaintenanceOutcome::Complete);
        assert!(trim.cleanup_pending);
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);

        slot.inject_failure_once(VirtualMemoryOperation::Deactivate);
        let retry = pool
            .inner
            .execute_maintenance(MaintenanceAction::RetryCleanup { generation: 1 });
        assert_eq!(retry, MaintenancePass::new(MaintenanceOutcome::Retry));
        assert_eq!(pool.inner.arena.diagnostics().cleanup_retries, 1);
        assert_eq!(pool.inner.arena.diagnostics().cleanup_failures, 1);

        let cleanup = pool
            .inner
            .execute_maintenance(MaintenanceAction::RetryCleanup { generation: 1 });
        assert_eq!(cleanup, MaintenancePass::new(MaintenanceOutcome::Complete));
        assert_eq!(pool.inner.arena.diagnostics().cleanup_retries, 2);
        assert_eq!(pool.inner.arena.diagnostics().cleanup_failures, 1);
        pool.assert_quiescent_zero();
        let reused = pool.acquire_unreserved(carrier_size).unwrap();
        drop(reused);
    }

    #[test]
    fn test_cleanup_request_starts_one_named_worker_lazily() {
        let (pool, _) = test_pool(1, 1);
        assert!(!pool.inner.maintenance.worker_started());

        pool.inner.maintenance.request_cleanup(&pool.inner);

        assert!(pool.inner.maintenance.worker_started());
        assert_eq!(
            pool.inner.maintenance.worker_name().as_deref(),
            Some(WORKER_NAME)
        );
        assert_eq!(pool.maintenance_spawn_attempts(), 1);
        let diagnostics = pool.diagnostics();
        assert_eq!(diagnostics.worker_starts, 1);
        assert_eq!(diagnostics.cleanup_requests, 1);
    }

    #[test]
    fn test_idle_deadline_wakes_worker_and_reclaims() {
        let (pool, carrier_size) = test_pool(1, 8);
        let owned = pool.acquire_unreserved(carrier_size * 8).unwrap();
        drop(owned);

        pool.inner.maintenance.record_idle_after(
            &pool.inner,
            Instant::now(),
            Duration::from_millis(10),
        );

        wait_until(|| {
            pool.metrics().prepared_capacity_bytes() == (carrier_size * 2) as u64
                && pool.diagnostics().reclaim_passes == 1
        });
        assert_eq!(pool.maintenance_spawn_attempts(), 1);
        let diagnostics = pool.diagnostics();
        assert_eq!(diagnostics.idle_deadlines, 1);
        assert_eq!(diagnostics.reclaim_passes, 1);
        assert_eq!(diagnostics.reclaim_retries, 0);
    }

    #[test]
    fn test_concurrent_requests_publish_one_worker() {
        const CALLERS: usize = 8;

        let (pool, _) = test_pool(1, 1);
        let barrier = Arc::new(Barrier::new(CALLERS));
        let mut callers = Vec::new();
        for _ in 0..CALLERS {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            callers.push(std::thread::spawn(move || {
                barrier.wait();
                pool.inner.maintenance.request_cleanup(&pool.inner);
            }));
        }
        for caller in callers {
            caller.join().unwrap();
        }

        assert!(pool.inner.maintenance.worker_started());
        assert_eq!(pool.maintenance_spawn_attempts(), 1);
    }

    #[test]
    fn test_spawn_failure_disables_only_maintenance() {
        let (pool, carrier_size) = test_pool(1, 1);
        pool.inject_maintenance_spawn_failure();

        pool.inner.maintenance.request_cleanup(&pool.inner);

        assert!(pool.inner.maintenance.is_disabled());
        assert!(!pool.inner.maintenance.worker_started());
        assert_eq!(pool.maintenance_spawn_attempts(), 1);
        assert_eq!(pool.diagnostics().worker_start_failures, 1);

        pool.inner.maintenance.record_idle(&pool.inner);
        assert_eq!(pool.maintenance_spawn_attempts(), 1);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        drop(owned);
    }

    #[test]
    fn test_waiting_worker_does_not_retain_the_pool() {
        let (pool, _) = test_pool(1, 1);
        pool.inner.maintenance.request_cleanup(&pool.inner);
        let weak = Arc::downgrade(&pool.inner);

        drop(pool);

        wait_until(|| weak.strong_count() == 0);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_worker_final_owner_does_not_join_itself() {
        let (pool, _) = test_pool(1, 1);
        let pause = pool.pause_maintenance_after_upgrade();
        pool.inner.maintenance.request_cleanup(&pool.inner);
        wait_until(|| pause.entered());
        let weak = Arc::downgrade(&pool.inner);

        drop(pool);
        pause.release();

        wait_until(|| weak.strong_count() == 0);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_scheduler_hooks_arm_and_cancel_one_idle_epoch() {
        let (pool, _) = test_pool(1, 1);

        pool.record_global_idle();
        assert!(pool
            .inner
            .maintenance
            .control
            .state
            .lock()
            .next_wake()
            .is_some());
        assert_eq!(pool.diagnostics().idle_deadlines, 1);

        pool.record_managed_activity();
        assert!(pool
            .inner
            .maintenance
            .control
            .state
            .lock()
            .next_wake()
            .is_none());
        assert_eq!(pool.diagnostics().idle_deadlines, 1);
    }

    #[test]
    fn test_preparation_failure_schedules_cleanup_and_quiesces() {
        let (pool, carrier_size) = test_pool(1, 1);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        drop(owned);
        let slot = pool
            .inner
            .arena
            .select_trim_candidate()
            .expect("free prepared slot");
        let reclaimed = pool.inner.execute_maintenance(MaintenanceAction::Reclaim {
            epoch: 0,
            target: CarrierCount::ZERO,
        });
        assert_eq!(reclaimed.outcome, MaintenanceOutcome::Complete);
        pool.assert_quiescent_zero();

        slot.inject_failure_once(VirtualMemoryOperation::Prepare);
        assert!(matches!(
            pool.acquire_unreserved(carrier_size),
            Err(super::super::AcquireError::PhysicalAllocationFailed)
        ));

        wait_until(|| pool.diagnostics().cleanup_retries == 1);
        let diagnostics = pool.diagnostics();
        assert_eq!(diagnostics.cleanup_requests, 1);
        assert_eq!(diagnostics.cleanup_failures, 0);
        assert_eq!(diagnostics.cleanup_pending_blocks, 0);
        pool.assert_quiescent_zero();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::buffer_pool::test_util::test_single_carrier_pool;
    use crate::runtime::sync::sync::{Arc, Mutex};
    use crate::runtime::sync::thread;

    #[test]
    fn test_activity_and_deadline_expiry_share_one_epoch_order() {
        loom::model(|| {
            let start = Instant::now();
            let state = Arc::new(Mutex::new(MaintenanceState::new()));
            state.lock().record_idle(start, Duration::ZERO);

            let active = Arc::clone(&state);
            let activity = thread::spawn(move || active.lock().record_activity());
            let expiring = Arc::clone(&state);
            let expiry = thread::spawn(move || {
                expiring
                    .lock()
                    .next_action(start, CarrierCount::new(4), CarrierCount::new(1))
            });

            activity.join().unwrap();
            let _ = expiry.join().unwrap();
            assert_eq!(
                state.lock().next_action(
                    start + Duration::from_secs(1),
                    CarrierCount::new(4),
                    CarrierCount::new(1),
                ),
                None
            );
        });
    }

    #[test]
    fn test_cleanup_completion_cannot_erase_a_new_request() {
        loom::model(|| {
            let start = Instant::now();
            let state = Arc::new(Mutex::new(MaintenanceState::new()));
            state.lock().request_cleanup(start);
            let action = state
                .lock()
                .next_action(start, CarrierCount::new(1), CarrierCount::new(1))
                .unwrap();

            let requesting = Arc::clone(&state);
            let request = thread::spawn(move || requesting.lock().request_cleanup(start));
            let finishing = Arc::clone(&state);
            let finish = thread::spawn(move || {
                finishing.lock().finish_action(
                    action,
                    MaintenanceOutcome::Complete,
                    start,
                    Duration::ZERO,
                )
            });

            request.join().unwrap();
            finish.join().unwrap();
            let next = state
                .lock()
                .next_action(start, CarrierCount::new(1), CarrierCount::new(1));
            assert!(next.is_some());
        });
    }

    #[test]
    fn test_claim_and_reclaim_compose_through_the_pool() {
        loom::model(|| {
            let (pool, carrier_size) = test_single_carrier_pool(1);
            let initial = pool.acquire_unreserved(carrier_size).unwrap();
            drop(initial);

            let reclaiming = pool.clone();
            let reclaim = thread::spawn(move || {
                reclaiming
                    .inner
                    .execute_maintenance(MaintenanceAction::Reclaim {
                        epoch: 0,
                        target: CarrierCount::ZERO,
                    })
            });
            let claiming = pool.clone();
            let claim = thread::spawn(move || claiming.acquire_unreserved(carrier_size));

            let _ = reclaim.join().unwrap();
            let owned = claim.join().unwrap().unwrap();
            drop(owned);
            assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        });
    }

    #[test]
    fn test_stop_and_wait_share_the_control_mutex() {
        loom::model(|| {
            let control = Arc::new(MaintenanceControl::new());
            let waiting = Arc::clone(&control);
            let waiter = thread::spawn(move || {
                let state = waiting.state.lock();
                if state.is_stopping() {
                    return;
                }
                let state = waiting.wake.wait(state);
                assert!(state.is_stopping());
            });

            control.state.lock().stop();
            control.wake.notify_all();
            waiter.join().unwrap();
        });
    }

    #[test]
    fn test_disable_cannot_publish_due_work() {
        loom::model(|| {
            let state = Arc::new(Mutex::new(MaintenanceState::new()));
            let disabling = Arc::clone(&state);
            let disable = thread::spawn(move || disabling.lock().disable());
            let requesting = Arc::clone(&state);
            let request = thread::spawn(move || {
                requesting.lock().request_cleanup(Instant::now());
            });

            disable.join().unwrap();
            request.join().unwrap();
            let mut state = state.lock();
            assert_eq!(
                state.next_action(Instant::now(), CarrierCount::new(1), CarrierCount::new(1),),
                None
            );
        });
    }
}
