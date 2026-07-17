/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer descriptor and related types for scheduler-managed transfer state.
//!
//! See the [`scheduler`](super) module docs for the threading and cost
//! model that the descriptor's claim protocol enforces.

use crate::runtime::sync::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::runtime::sync::sync::Arc;

use tokio::sync::Notify;

use crate::transfer::{BoxTransfer, Transfer, TransferId};

/// Claim + wake-requested protocol primitives.
///
/// Scoped to a sub-module so the underlying atomics are sourced from the
/// `runtime/sync` compat layer (loom under `cfg(s3_tm_loom)`, std otherwise)
/// without forcing the rest of this file's atomics onto the same path.
///
/// # Protocol
///
/// The claim flag tracks whether a descriptor is queued in the ready set
/// or currently being polled. Asserted by [`ClaimState::try_claim`], cleared
/// by [`ClaimState::release_claim`]. The flag is held continuously from
/// ready-set insert through `pop` through `poll_work` until `generate_work`
/// finishes handling the outcome - this is what serializes `poll_work` to
/// one worker per descriptor at a time.
///
/// The wake-requested flag is set unconditionally by `Scheduler::wake`
/// whether or not the claim CAS succeeded, and consumed by `generate_work`
/// in a release-and-recheck pattern. The pairing closes the lost-wake race
/// where a wake arrives between `release_claim` and the descriptor leaving
/// the ready set.
///
/// All four operations are `SeqCst` so the release-and-recheck of the claim
/// flag composes correctly with the mark-and-try-claim of the wake flag.
/// These two pairs operate on independent atomics; without a global total
/// order, a schedule exists on weak memory models where neither side
/// observes the other and the wake is lost.
pub(super) mod claim {
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};

    pub(in crate::scheduler) struct ClaimState {
        claimed: AtomicBool,
        wake_requested: AtomicBool,
    }

    impl ClaimState {
        pub(in crate::scheduler) fn new() -> Self {
            Self {
                claimed: AtomicBool::new(false),
                wake_requested: AtomicBool::new(false),
            }
        }

        /// Atomically take the claim if it is currently free. Returns
        /// `true` on success.
        pub(in crate::scheduler) fn try_claim(&self) -> bool {
            // compare_exchange rather than swap: on failure we must NOT
            // overwrite the existing `true`. A bare swap loses ownership
            // information when two callers race.
            self.claimed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }

        /// Release the claim. Pairs with `try_claim`.
        pub(in crate::scheduler) fn release_claim(&self) {
            self.claimed.store(false, Ordering::SeqCst);
        }

        /// Mark that a wake has been requested.
        pub(in crate::scheduler) fn mark_wake_requested(&self) {
            self.wake_requested.store(true, Ordering::SeqCst);
        }

        /// Atomically clear and return the wake-requested flag.
        pub(in crate::scheduler) fn take_wake_requested(&self) -> bool {
            self.wake_requested.swap(false, Ordering::SeqCst)
        }
    }
}

use claim::ClaimState;

/// Default priority assigned to new transfers
const DEFAULT_PRIORITY: u8 = 128;

/// Vruntime cost charged when a transfer dispatches one unit of real IO work
/// (a network/disk request).
pub(super) const IO_WORK_COST: u64 = 128;

/// Vruntime cost charged when a composite *spawns a child* (as opposed to
/// dispatching real IO). A spawn is cheap coordination — it enqueues a
/// schedulable child but performs no IO itself — so it is charged less than an
/// IO unit. Charging spawn the full [`IO_WORK_COST`] makes a composite parent
/// accumulate vruntime as fast as its own children, so CFS stops selecting the
/// parent after a few spawns and it loses the poll-turns it needs to keep the
/// pipeline filled. A reduced spawn cost keeps the parent scheduling-competitive
/// with its children so it can refill continuously. This integrates spawn
/// cadence *into* CFS rather than governing it with ad-hoc spawn-batch-size
/// constants.
///
/// Charged to both individual and group vruntime (like all work). Composites
/// with the same spawn-to-IO ratio charge spawn identically, so their relative
/// `group_vruntime` ordering is unchanged. Composites with different ratios
/// accrue group vruntime at different rates per dispatched IO, producing a
/// bounded cross-group dispatch skew that penalizes the spawn-heavier group —
/// self-limiting, since a group cannot spawn its way to a larger dispatch
/// share, only a smaller one.
///
/// The `SPAWN_WORK_COST : IO_WORK_COST` ratio sets the steady-state in-flight
/// level: a smaller spawn cost lets a composite parent win more poll turns and
/// hold more children in flight.
pub(super) const SPAWN_WORK_COST: u64 = IO_WORK_COST / 2;

/// Precision-preserving scale factor for priority-weighted vruntime deltas.
///
/// `delta = (IO_WORK_COST * PRIORITY_SCALE) / priority`. Without scaling, integer
/// division would collapse to zero for priorities above `IO_WORK_COST` (e.g.,
/// priority 200 with `IO_WORK_COST = 128` would yield `delta = 0`, starving all
/// peers). Scaling by 256 keeps the formula resolution-correct across the
/// full `u8` priority range (1..=255). Mirrors the role of `NICE_0_LOAD` in
/// Linux CFS.
pub(super) const PRIORITY_SCALE: u64 = 256;

/// Compute the vruntime delta for a given priority at the full [`IO_WORK_COST`].
///
/// Higher priority yields a smaller delta, so the descriptor accumulates
/// vruntime more slowly and wins more dispatch share. Used for an individual
/// transfer's vruntime ([`TransferDescriptor::work_generated`]) and, via the
/// same charge, its group's vruntime when it generates work.
pub(super) fn vruntime_delta_for_priority(priority: u8) -> u64 {
    vruntime_delta_for_cost(IO_WORK_COST, priority)
}

/// Compute the vruntime delta for a given work cost and priority.
///
/// `delta = (cost * PRIORITY_SCALE) / priority`. Generalizes
/// [`vruntime_delta_for_priority`] so the spawn path can charge the reduced
/// [`SPAWN_WORK_COST`] while preserving the same priority scaling.
pub(super) fn vruntime_delta_for_cost(cost: u64, priority: u8) -> u64 {
    (cost * PRIORITY_SCALE) / (priority as u64).max(1)
}

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
    /// Shared with the GroupQueue this descriptor belongs to. When
    /// `work_generated` fires, both individual vruntime and group
    /// vruntime advance by the same priority-scaled delta.
    group_vruntime: Arc<AtomicU64>,
    queued_executing: QueuedExecuting,
    transfer: BoxTransfer,
    idle_notify: Notify,
    claim_state: ClaimState,
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
    #[cfg(test)]
    pub(crate) fn new(transfer: BoxTransfer) -> Self {
        Self::new_with_vruntime(transfer, 0, Arc::new(AtomicU64::new(0)))
    }

    pub(crate) fn new_with_vruntime(
        transfer: BoxTransfer,
        initial_vruntime: u64,
        group_vruntime: Arc<AtomicU64>,
    ) -> Self {
        Self(Arc::new(Inner {
            priority: AtomicU8::new(DEFAULT_PRIORITY),
            vruntime: AtomicU64::new(initial_vruntime),
            group_vruntime,
            queued_executing: QueuedExecuting::new(),
            transfer,
            idle_notify: Notify::new(),
            claim_state: ClaimState::new(),
        }))
    }

    /// Atomically take the claim if free. Returns `true` on success.
    pub(super) fn try_claim(&self) -> bool {
        self.0.claim_state.try_claim()
    }

    /// Release the claim.
    pub(super) fn release_claim(&self) {
        self.0.claim_state.release_claim()
    }

    /// Mark that a wake has been requested.
    pub(super) fn mark_wake_requested(&self) {
        self.0.claim_state.mark_wake_requested()
    }

    /// Atomically clear and return the wake-requested flag.
    pub(super) fn take_wake_requested(&self) -> bool {
        self.0.claim_state.take_wake_requested()
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

    pub(super) fn group_vruntime_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.0.group_vruntime)
    }

    pub(crate) fn vruntime(&self) -> u64 {
        self.0.vruntime.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_vruntime(&self, vruntime: u64) {
        self.0.vruntime.store(vruntime, Ordering::Release);
    }

    fn add_vruntime(&self, delta: u64) {
        self.0.vruntime.fetch_add(delta, Ordering::AcqRel);
        self.0.group_vruntime.fetch_add(delta, Ordering::SeqCst);
    }

    /// Record work generation and update vruntime based on priority.
    /// Higher priority = slower vruntime accumulation = more work share.
    pub(crate) fn work_generated(&self) {
        let delta = vruntime_delta_for_priority(self.priority());
        self.add_vruntime(delta);
    }

    /// Record a *spawn* (a composite enqueued a child) and update vruntime at
    /// the reduced [`SPAWN_WORK_COST`] rather than the full [`IO_WORK_COST`].
    /// Charges both individual and group vruntime (via [`Self::add_vruntime`]),
    /// so the discount is uniform across composites and preserves cross-group
    /// fairness. Keeps a composite parent scheduling-competitive with its own
    /// children so it can refill the pipeline continuously instead of in bursts.
    pub(crate) fn work_generated_spawn(&self) {
        let delta = vruntime_delta_for_cost(SPAWN_WORK_COST, self.priority());
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
            // Register interest before checking
            let notified = self.0.idle_notify.notified();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }
}

/// Packed atomic counter for queued + executing counts.
///
/// A single `AtomicU64` instead of two `AtomicU32`s so that `start_executing`
/// (queued-1, executing+1) is a single CAS - no window where the counts are
/// inconsistent and `is_idle()` could return a false positive.
///
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

    #[cfg(test)]
    fn get(&self) -> (u32, u32) {
        let val = self.0.load(Ordering::Acquire);
        ((val >> 32) as u32, val as u32)
    }

    #[cfg(test)]
    fn outstanding(&self) -> u64 {
        let (q, e) = self.get();
        q as u64 + e as u64
    }
}

/// RAII guard for a descriptor's claim. On drop, releases the claim
/// unless [`hold`](Self::hold) was called to consume the guard without
/// releasing. Used by the scheduler's `generate_work` to ensure the
/// claim is released on every exit path, including panic.
pub(super) struct ClaimGuard<'a> {
    desc: &'a TransferDescriptor,
    released: bool,
}

impl<'a> ClaimGuard<'a> {
    /// Wrap a descriptor whose claim is currently held. The caller is
    /// responsible for having taken the claim (via `try_claim` or `pop`).
    pub(super) fn new(desc: &'a TransferDescriptor) -> Self {
        Self {
            desc,
            released: false,
        }
    }

    /// Explicit release now; subsequent drop is a no-op.
    pub(super) fn release(mut self) {
        self.desc.release_claim();
        self.released = true;
    }

    /// Consume the guard without releasing - the claim stays asserted.
    /// Used by the `PollWork::Ready` path which keeps the claim held
    /// across re-insert.
    pub(super) fn hold(mut self) {
        self.released = true;
    }
}

impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.desc.release_claim();
        }
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
        #[test]
        #[should_panic(expected = "queued underflow")]
        #[cfg(debug_assertions)]
        fn test_decrement_queued_underflow_panics() {
            QueuedExecuting::new().decrement_queued(1);
        }

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
        #[test]
        #[should_panic(expected = "executing underflow")]
        #[cfg(debug_assertions)]
        fn test_finish_executing_underflow_panics() {
            QueuedExecuting::new().finish_executing();
        }

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
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

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
        #[test]
        fn test_work_generated_spawn_charges_both_individual_and_group_vruntime() {
            let desc = test_descriptor(1);
            let group = desc.group_vruntime_arc();
            assert_eq!(desc.vruntime(), 0);
            assert_eq!(group.load(Ordering::SeqCst), 0);

            desc.work_generated_spawn();

            // A spawn charges the reduced spawn cost to BOTH the individual and
            // the group vruntime. The group charge is what makes a composite's
            // spawn cadence visible to cross-group CFS fairness; dropping it
            // (charging individual only) would silently change cross-group
            // dispatch share for composites with differing spawn:IO ratios.
            let expected = vruntime_delta_for_cost(SPAWN_WORK_COST, desc.priority());
            assert_eq!(
                desc.vruntime(),
                expected,
                "spawn must charge individual vruntime by the spawn delta"
            );
            assert_eq!(
                group.load(Ordering::SeqCst),
                expected,
                "spawn must charge group vruntime by the same spawn delta"
            );
        }

        // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
        #[cfg_attr(miri, ignore)]
        #[test]
        fn test_spawn_charge_is_less_than_io_charge() {
            // The spawn discount is what keeps a composite parent scheduling-
            // competitive with its own children. If SPAWN_WORK_COST ever equaled
            // IO_WORK_COST the parent would accumulate vruntime as fast as the
            // work it dispatches and lose the poll turns it needs to refill.
            let spawn = test_descriptor(1);
            let io = test_descriptor(2);

            spawn.work_generated_spawn();
            io.work_generated();

            assert!(
                spawn.vruntime() < io.vruntime(),
                "spawn charge ({}) must be less than IO charge ({})",
                spawn.vruntime(),
                io.vruntime()
            );
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::claim::ClaimState;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// After any interleaving of the release-and-recheck pattern with a
    /// concurrent wake arrival, at least one of the two threads must own
    /// the re-insert. If neither does, a wake is outstanding but the
    /// descriptor is absent from the ready set and the transfer is stuck.
    #[test]
    fn release_recheck_no_lost_wake() {
        loom::model(|| {
            // Start state: claim held by A (simulating post-poll_work Pending).
            let state = Arc::new(ClaimState::new());
            assert!(state.try_claim());

            let inserted = Arc::new(AtomicBool::new(false));

            let s2 = state.clone();
            let i2 = inserted.clone();
            let a = thread::spawn(move || {
                s2.release_claim();
                if s2.take_wake_requested() {
                    // generate_work sees wake; tries to reclaim + insert.
                    if s2.try_claim() {
                        i2.store(true, Ordering::SeqCst);
                    }
                }
            });

            let s3 = state.clone();
            let i3 = inserted.clone();
            let b = thread::spawn(move || {
                s3.mark_wake_requested();
                if s3.try_claim() {
                    i3.store(true, Ordering::SeqCst);
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            assert!(
                inserted.load(Ordering::SeqCst),
                "lost wake: neither A nor B claimed the descriptor to re-insert it"
            );
        });
    }

    /// Single-poll exclusivity: two threads concurrently calling
    /// `ClaimState::try_claim` must never both succeed. The ready-set uses
    /// this to guarantee at most one thread inside `poll_work(desc)` at a
    /// time.
    #[test]
    fn try_claim_is_exclusive() {
        loom::model(|| {
            let state = Arc::new(ClaimState::new());

            let s2 = state.clone();
            let a = thread::spawn(move || s2.try_claim());
            let s3 = state.clone();
            let b = thread::spawn(move || s3.try_claim());

            let got_a = a.join().unwrap();
            let got_b = b.join().unwrap();

            assert!(got_a ^ got_b, "try_claim was not exclusive");
        });
    }
}
