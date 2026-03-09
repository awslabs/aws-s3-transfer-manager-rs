/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer descriptor and related types for scheduler-managed transfer state.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::transfer::{BoxTransfer, Transfer, TransferId};

/// Default priority assigned to new transfers
const DEFAULT_PRIORITY: u8 = 128;
/// Fixed work cost
const WORK_COST: u64 = 128;

/// The scheduler's handle to a transfer.
///
/// Clone is cheap (Arc wrapper). Contains all metadata for scheduling:
/// - Identity and priority
/// - Virtual runtime for CFS-style fair scheduling  
/// - Outstanding work tracking (queued + executing)
/// - The transfer itself for polling work
#[derive(Clone)]
pub(crate) struct TransferDescriptor(Arc<Inner>);

struct Inner {
    priority: AtomicU8,
    vruntime: AtomicU64,
    queued_executing: QueuedExecuting,
    transfer: BoxTransfer,
    idle_notify: Notify,
}

impl std::fmt::Debug for TransferDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferDescriptor")
            .field("tid", &self.0.transfer.ctx().id)
            .field("priority", &self.priority())
            .field("vruntime", &self.vruntime())
            .finish_non_exhaustive()
    }
}

impl TransferDescriptor {
    #[cfg(test)] // TODO(phase3): evaluate for public scheduling API
    pub(crate) fn new(transfer: BoxTransfer) -> Self {
        Self::new_with_vruntime(transfer, 0)
    }

    pub(crate) fn new_with_vruntime(transfer: BoxTransfer, initial_vruntime: u64) -> Self {
        Self(Arc::new(Inner {
            priority: AtomicU8::new(DEFAULT_PRIORITY),
            vruntime: AtomicU64::new(initial_vruntime),
            queued_executing: QueuedExecuting::new(),
            transfer,
            idle_notify: Notify::new(),
        }))
    }

    pub(crate) fn transfer(&self) -> &dyn Transfer {
        self.0.transfer.as_ref()
    }

    pub(crate) fn id(&self) -> TransferId {
        self.0.transfer.ctx().id
    }

    pub(crate) fn priority(&self) -> u8 {
        self.0.priority.load(Ordering::Acquire)
    }

    pub(crate) fn set_priority(&self, priority: u8) {
        self.0.priority.store(priority, Ordering::Release);
    }

    pub(crate) fn vruntime(&self) -> u64 {
        self.0.vruntime.load(Ordering::Acquire)
    }

    #[cfg(test)] // TODO(phase3): evaluate for public scheduling API
    pub(crate) fn set_vruntime(&self, vruntime: u64) {
        self.0.vruntime.store(vruntime, Ordering::Release);
    }

    fn add_vruntime(&self, delta: u64) {
        self.0.vruntime.fetch_add(delta, Ordering::AcqRel);
    }

    /// Record work generation and update vruntime based on priority.
    /// Higher priority = slower vruntime accumulation = more work share.
    pub(crate) fn work_generated(&self) {
        let priority = self.priority() as u64;
        // Scale up before dividing to avoid integer division truncating to zero.
        // Multiplying by 256 ensures max priority (255) still yields delta >= 1.
        let delta = (WORK_COST * 256) / priority.max(1);
        self.add_vruntime(delta);
    }

    /// Called when work is enqueued (to be executed)
    pub(crate) fn work_queued(&self) {
        self.0.queued_executing.increment_queued();
    }

    /// Called when worker picks up work (atomic queued-- / executing++)
    pub(crate) fn work_started(&self) {
        self.0.queued_executing.start_executing();
    }

    /// Called when worker completes work. Returns true if transfer is now idle.
    pub(crate) fn work_finished(&self) -> bool {
        self.0.queued_executing.finish_executing()
    }

    /// Called when queued work is purged (cancelled before execution)
    pub(crate) fn work_purged(&self, count: usize) {
        self.0.queued_executing.decrement_queued(count as u32);
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.0.queued_executing.is_idle()
    }

    /// Wake anyone waiting on this transfer to become idle
    pub(crate) fn notify_idle(&self) {
        self.0.idle_notify.notify_waiters();
    }

    /// Check if this transfer has reached a terminal state and no longer needs polled
    pub(crate) fn is_terminal(&self) -> bool {
        !self.0.transfer.ctx().is_active()
    }

    pub(crate) async fn wait_for_idle(&self) {
        loop {
            if self.is_idle() {
                return;
            }
            self.0.idle_notify.notified().await;
        }
    }
}

/// Packed atomic counter for queued + executing counts.
/// Layout: `[queued: u32][executing: u32]`
#[derive(Debug, Default)]
struct QueuedExecuting(AtomicU64);

impl QueuedExecuting {
    const QUEUED_ONE: u64 = 1 << 32;
    const EXECUTING_ONE: u64 = 1;
    /// Subtract 1 from queued (upper), add 1 to executing (lower)
    const QUEUED_TO_EXECUTING: u64 = Self::EXECUTING_ONE.wrapping_sub(Self::QUEUED_ONE);

    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn increment_queued(&self) {
        let prev = self.0.fetch_add(Self::QUEUED_ONE, Ordering::AcqRel);
        debug_assert!((prev >> 32) < u32::MAX as u64, "queued overflow");
    }

    fn decrement_queued(&self, count: u32) {
        let delta = (count as u64) << 32;
        let prev = self.0.fetch_sub(delta, Ordering::AcqRel);
        debug_assert!((prev >> 32) >= count as u64, "queued underflow");
    }

    fn start_executing(&self) {
        let prev = self
            .0
            .fetch_add(Self::QUEUED_TO_EXECUTING, Ordering::AcqRel);
        debug_assert!((prev >> 32) > 0, "queued underflow in start_executing");
    }

    fn finish_executing(&self) -> bool {
        let prev = self.0.fetch_sub(Self::EXECUTING_ONE, Ordering::AcqRel);
        debug_assert!((prev as u32) > 0, "executing underflow");
        prev == Self::EXECUTING_ONE
    }

    fn is_idle(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }

    #[cfg(test)] // TODO(phase3): evaluate for public scheduling API
    fn get(&self) -> (u32, u32) {
        let val = self.0.load(Ordering::Acquire);
        ((val >> 32) as u32, val as u32)
    }

    #[cfg(test)] // TODO(phase3): evaluate for public scheduling API
    fn outstanding(&self) -> u64 {
        let (q, e) = self.get();
        q as u64 + e as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod queued_executing {
        use super::*;

        #[test]
        fn test_new_is_idle() {
            let qe = QueuedExecuting::new();
            assert!(qe.is_idle());
            assert_eq!(qe.get(), (0, 0));
            assert_eq!(qe.outstanding(), 0);
        }

        #[test]
        fn test_increment_queued() {
            let qe = QueuedExecuting::new();
            qe.increment_queued();
            assert_eq!(qe.get(), (1, 0));
            assert!(!qe.is_idle());

            qe.increment_queued();
            assert_eq!(qe.get(), (2, 0));
        }

        #[test]
        fn test_start_executing() {
            let qe = QueuedExecuting::new();
            qe.increment_queued();
            qe.increment_queued();
            qe.start_executing();
            assert_eq!(qe.get(), (1, 1));
        }

        #[test]
        fn test_finish_executing_returns_idle_status() {
            let qe = QueuedExecuting::new();
            qe.increment_queued();
            qe.start_executing();
            assert!(qe.finish_executing());
            assert!(qe.is_idle());
        }

        #[test]
        fn test_finish_executing_not_idle_when_queued_remains() {
            let qe = QueuedExecuting::new();
            qe.increment_queued();
            qe.increment_queued();
            qe.start_executing();
            assert!(!qe.finish_executing());
            assert_eq!(qe.get(), (1, 0));
        }

        #[test]
        fn test_full_lifecycle() {
            let qe = QueuedExecuting::new();
            qe.increment_queued();
            qe.increment_queued();
            qe.increment_queued();
            qe.start_executing();
            qe.start_executing();
            assert!(!qe.finish_executing());
            qe.decrement_queued(1);
            assert!(qe.finish_executing());
            assert!(qe.is_idle());
        }

        #[test]
        #[should_panic(expected = "queued underflow")]
        #[cfg(debug_assertions)]
        fn test_decrement_queued_underflow_panics() {
            QueuedExecuting::new().decrement_queued(1);
        }

        #[test]
        #[should_panic(expected = "executing underflow")]
        #[cfg(debug_assertions)]
        fn test_finish_executing_underflow_panics() {
            QueuedExecuting::new().finish_executing();
        }

        #[test]
        #[should_panic(expected = "queued underflow in start_executing")]
        #[cfg(debug_assertions)]
        fn test_start_executing_without_queued_panics() {
            QueuedExecuting::new().start_executing();
        }
    }

    mod priority_vruntime {
        use super::*;
        use crate::scheduler::transfer::mock::FixedWorkCount;
        use crate::scheduler::MockTransfer;
        use std::sync::Arc;

        fn test_descriptor(id: u64) -> TransferDescriptor {
            let transfer_id = TransferId { id, parent: None };
            let sm = Arc::new(FixedWorkCount::new(100));
            let transfer = Box::new(MockTransfer::new(transfer_id, sm));
            TransferDescriptor::new(transfer)
        }

        #[test]
        fn test_priority_affects_vruntime_accumulation() {
            let high = test_descriptor(1);
            let low = test_descriptor(2);

            high.set_priority(255); // highest
            low.set_priority(64); // low

            // Simulate 10 work items each
            for _ in 0..10 {
                high.work_generated();
                low.work_generated();
            }

            // Higher priority should accumulate LESS vruntime
            // (lower vruntime = scheduled first = more work share)
            let high_vrt = high.vruntime();
            let low_vrt = low.vruntime();

            assert!(
                high_vrt < low_vrt,
                "high priority should have lower vruntime: high={}, low={}",
                high_vrt,
                low_vrt
            );
        }

        #[test]
        fn test_max_priority_still_accumulates_vruntime() {
            let desc = test_descriptor(1);
            desc.set_priority(255); // max priority

            desc.work_generated();

            // Even max priority should accumulate SOME vruntime
            // Otherwise it would starve all other transfers
            assert!(
                desc.vruntime() > 0,
                "max priority should still accumulate vruntime, got {}",
                desc.vruntime()
            );
        }

        #[test]
        fn test_priority_ratio_reflected_in_vruntime() {
            let high = test_descriptor(1);
            let low = test_descriptor(2);

            high.set_priority(200);
            low.set_priority(50); // 4x lower priority

            // Same number of work items
            for _ in 0..100 {
                high.work_generated();
                low.work_generated();
            }

            let high_vrt = high.vruntime();
            let low_vrt = low.vruntime();

            // Low priority should accumulate roughly 4x more vruntime
            // Allow some tolerance for integer math
            let ratio = low_vrt as f64 / high_vrt.max(1) as f64;
            assert!(
                ratio > 2.0,
                "4x priority difference should yield >2x vruntime ratio: high={}, low={}, ratio={}",
                high_vrt,
                low_vrt,
                ratio
            );
        }
    }
}
