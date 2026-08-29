/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Idle reclamation policy and maintenance-worker coordination.
//!
//! [`MaintenanceState`] converts explicit activity, deadline, and completion
//! events into serialized maintenance actions. Pool and platform operations
//! execute outside this state machine.

use std::time::{Duration, Instant};

use super::CarrierCount;

/// Fraction of the configured block ceiling retained after an idle deadline.
const IDLE_RETENTION_DIVISOR: usize = 4;

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
    /// original deadline and cannot extend cache retention.
    pub(super) fn record_idle(&mut self, now: Instant, idle_timeout: Duration) {
        if self.stopping || self.disabled || self.reclaim.is_some() {
            return;
        }
        if self
            .idle_deadline
            .is_some_and(|deadline| deadline.epoch == self.activity_epoch)
        {
            return;
        }
        let expires_at = now
            .checked_add(idle_timeout)
            .unwrap_or_else(|| super::invariant_violation("maintenance idle deadline overflow"));
        self.idle_deadline = Some(IdleDeadline {
            epoch: self.activity_epoch,
            expires_at,
        });
    }

    /// Requests mapping cleanup without losing a concurrent newer request.
    pub(super) fn request_cleanup(&mut self, now: Instant) {
        if self.stopping || self.disabled {
            return;
        }
        let generation = self.next_cleanup_generation;
        self.next_cleanup_generation = self.next_cleanup_generation.wrapping_add(1);
        self.cleanup = Some(CleanupRequest {
            generation,
            eligible_at: now,
        });
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

    const IDLE: Duration = Duration::from_secs(10);
    const RETRY: Duration = Duration::from_secs(2);

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
            .next_action(start + IDLE, carriers(256), carriers(64))
            .unwrap();
        state.finish_action(action, MaintenanceOutcome::Retry, start + IDLE, RETRY);

        assert_eq!(
            state.next_action(start + IDLE + RETRY, carriers(64), carriers(64)),
            Some(MaintenanceAction::Reclaim {
                epoch: 0,
                target: carriers(64),
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
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
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
}
