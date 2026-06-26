/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Admission control for [`Scheduler::generate_work`](crate::scheduler::Scheduler).
//!
//! Holds no scheduler state; the caller supplies the work-generation pass. The
//! atomic comes from the `runtime::sync` compat layer so the type compiles
//! against `loom` under `cfg(s3_tm_loom)` and is model-checked in `loom_tests`.

use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};

/// Bit 0 of the packed state: set while a runner holds the role.
const RUNNING: usize = 1;
/// Increment applied to the packed state to bump the request epoch (bits 1..).
const EPOCH_STEP: usize = 2;

/// Outcome of [`GateState::acquire`].
enum Acquire {
    /// This caller became the sole runner; it observed the gate at `epoch` and
    /// must drive passes until [`GateState::release`] reports `Idle`.
    Runner { epoch: usize },
    /// A runner was already active; this caller bumped the epoch to record its
    /// request and the active runner will drain an extra pass for it.
    Recorded,
}

/// Outcome of [`GateState::release`].
enum Release {
    /// No request arrived since the observed epoch; the gate returned to idle
    /// and the runner role is released. Stop draining.
    Idle,
    /// A request bumped the epoch during the pass; the role is retained. Carries
    /// the new epoch to re-sync, then drain again.
    Pending(usize),
}

/// The packed runner/epoch atomic, with the bit-level state machine isolated
/// from the [`GenerateWorkGate`] role policy that wraps it.
///
/// One `usize` holds both: bit 0 is the `RUNNING` flag, bits 1.. are a monotonic
/// request *epoch*.
///
/// ```text
///  ┌──────────────── usize ─────────────────┐
///  │  request epoch (bits 1..)         │ R  │   R = RUNNING (bit 0)
///  └───────────────────────────────────┴────┘
/// ```
///
/// A caller that finds a runner active bumps the epoch rather than setting a
/// shared flag, so two concurrent requests are *counted*, not coalesced into one
/// bit. (A single saturating bit loses the second of two concurrent requests: the
/// runner services it once and releases, stranding the other — the failure that
/// motivated the epoch.)
///
/// All transitions are `SeqCst`. The epoch occupies all but the low bit and may
/// wrap; this is benign for an eventcount. A lost wake would require the epoch
/// to advance by exactly a multiple of `2^(usize::BITS-1)` between a pass ending
/// and its release CAS — i.e. that many concurrent requests in that window —
/// which cannot occur.
struct GateState {
    packed: AtomicUsize,
}

impl GateState {
    fn new() -> Self {
        Self {
            packed: AtomicUsize::new(0),
        }
    }

    /// Become the sole runner (epoch unchanged), or record a request by bumping
    /// the epoch. The CAS-retry loop terminates: a retry happens only when some
    /// other thread's CAS succeeded, so the system made progress.
    fn acquire(&self) -> Acquire {
        loop {
            let s = self.packed.load(Ordering::SeqCst);
            if s & RUNNING == 0 {
                // Idle: claim the runner role, leaving the epoch unchanged.
                if self
                    .packed
                    .compare_exchange_weak(s, s | RUNNING, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Acquire::Runner { epoch: s >> 1 };
                }
            } else {
                // A runner is active: record a request by bumping the epoch.
                if self
                    .packed
                    .compare_exchange_weak(s, s + EPOCH_STEP, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Acquire::Recorded;
                }
            }
            // CAS failed (weak spurious failure, or the state changed under us);
            // re-observe and retry.
        }
    }

    /// Release the runner role iff no request arrived since `observed`; otherwise
    /// return the new epoch so the runner re-syncs and drains again. The release
    /// is a CAS gated on the epoch — never a blind store — so a request that
    /// raced the release is always observed, not stomped.
    fn release(&self, observed: usize) -> Release {
        let expected = (observed << 1) | RUNNING;
        let idle = observed << 1; // clear RUNNING, same epoch
        match self
            .packed
            .compare_exchange(expected, idle, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => Release::Idle,
            Err(actual) => Release::Pending(actual >> 1),
        }
    }

    /// Clear `RUNNING` (preserving the epoch). Panic-path only; see
    /// [`GenerateWorkPermit`]'s `Drop`.
    ///
    /// A single atomic `fetch_and`, not a load/store pair: an epoch bump racing
    /// this clear is preserved rather than stomped by a stale read-back.
    fn force_idle(&self) {
        self.packed.fetch_and(!RUNNING, Ordering::SeqCst);
    }
}

/// Admits one work-generation runner at a time and counts concurrent requests
/// so none is lost.
///
/// Single-runner admission keeps ready-set pops sequential: a transfer gets the
/// chance to re-insert between pops, so priority ordering holds (parallel pops
/// would short-circuit the pop-poll-reinsert cycle priority depends on). A
/// caller that cannot acquire the permit records its request (via the epoch; see
/// [`GateState`]), and the active runner drains an extra pass for it.
///
/// No-lost-wake guarantee: the runner captures the epoch when it acquires and
/// re-syncs it after each pass; it releases only via a CAS that requires the
/// epoch to be unchanged ([`GateState::release`]). A request that bumps the
/// epoch makes that CAS fail, so the runner observes it and drains again — the
/// release is never a blind store. The bumping caller publishes its work
/// *before* bumping, so the runner's failing CAS reads-from the bump and the
/// subsequent pass observes the published work.
pub(in crate::scheduler) struct GenerateWorkGate {
    state: GateState,
}

impl GenerateWorkGate {
    pub(in crate::scheduler) fn new() -> Self {
        Self {
            state: GateState::new(),
        }
    }

    /// Try to become the sole work-generation runner.
    ///
    /// Returns `Some(permit)` if this caller acquired the runner role and
    /// must drive generation passes until [`GenerateWorkPermit::try_release`]
    /// reports the permit released. Returns `None` if a runner is already
    /// active — the call has bumped the request epoch so that runner drains at
    /// least one more pass, so the caller's just-published work is not
    /// stranded.
    ///
    /// Callers MUST publish their work (insert into the ready set) BEFORE
    /// calling this; otherwise the extra pass may run before the work is
    /// visible. `None` is also the re-entrancy guard: a re-entrant call on the
    /// runner's own thread bumps the epoch and returns rather than recursing
    /// into a nested pass.
    pub(in crate::scheduler) fn try_acquire(&self) -> Option<GenerateWorkPermit<'_>> {
        match self.state.acquire() {
            Acquire::Runner { epoch } => Some(GenerateWorkPermit {
                gate: self,
                seen_epoch: epoch,
                released: false,
            }),
            Acquire::Recorded => None,
        }
    }
}

/// Proof that the holder is the sole active work-generation runner.
///
/// Held across the runner's whole drain loop — many passes, not one — and
/// relinquished by [`try_release`](Self::try_release) returning `true`,
/// unlike a semaphore permit that releases on drop. Carries `seen_epoch`, the
/// request epoch the runner has observed; a retire succeeds only if no newer
/// request has bumped past it. `Drop` is the panic-safety fallback only: if a
/// pass unwinds while the permit is still held, it clears `RUNNING` so
/// generation can resume (otherwise the runner role stays held and every future
/// caller bails forever, wedging the scheduler). A clean `try_release` disarms
/// the drop.
pub(in crate::scheduler) struct GenerateWorkPermit<'a> {
    gate: &'a GenerateWorkGate,
    /// The request epoch this runner has observed and serviced. Updated by
    /// `try_release` whenever a racing request bumps the gate's epoch.
    seen_epoch: usize,
    /// Set once `try_release` reports the permit released; suppresses the
    /// `Drop` reset so it cannot stomp a runner that has since acquired.
    released: bool,
}

impl GenerateWorkPermit<'_> {
    /// Call after each generation pass. Returns `true` if the permit was
    /// released — no request arrived since the last sync, stop draining.
    /// Returns `false` if a request arrived during the pass — re-syncs the
    /// observed epoch, keep the permit, and run another pass. Takes `&mut self`
    /// because on `false` the permit is retained with the updated epoch.
    pub(in crate::scheduler) fn try_release(&mut self) -> bool {
        match self.gate.state.release(self.seen_epoch) {
            Release::Idle => {
                self.released = true;
                true
            }
            Release::Pending(new_epoch) => {
                self.seen_epoch = new_epoch;
                false
            }
        }
    }
}

impl Drop for GenerateWorkPermit<'_> {
    fn drop(&mut self) {
        // A clean release already retired the role; only reset here on an
        // unwind, so a panicking pass does not wedge generation at RUNNING.
        if !self.released {
            self.gate.state.force_idle();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_idle_epoch_zero() {
        let g = GateState::new();
        assert_eq!(g.packed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn acquire_from_idle_becomes_runner() {
        let g = GateState::new();
        match g.acquire() {
            Acquire::Runner { epoch } => assert_eq!(epoch, 0),
            Acquire::Recorded => panic!("expected Runner"),
        }
        // State should be RUNNING, epoch 0.
        assert_eq!(g.packed.load(Ordering::Relaxed), RUNNING);
    }

    #[test]
    fn acquire_while_running_records_and_bumps_epoch() {
        let g = GateState::new();
        let Acquire::Runner { .. } = g.acquire() else {
            panic!("expected Runner");
        };
        // Second acquire while running → Recorded, epoch bumped.
        match g.acquire() {
            Acquire::Recorded => {}
            Acquire::Runner { .. } => panic!("expected Recorded"),
        }
        // State: RUNNING + epoch 1.
        assert_eq!(g.packed.load(Ordering::Relaxed), RUNNING | (1 << 1));
    }

    #[test]
    fn release_at_same_epoch_goes_idle() {
        let g = GateState::new();
        let Acquire::Runner { epoch } = g.acquire() else {
            panic!("expected Runner");
        };
        match g.release(epoch) {
            Release::Idle => {}
            Release::Pending(_) => panic!("expected Idle"),
        }
        assert_eq!(g.packed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn release_at_stale_epoch_reports_pending() {
        let g = GateState::new();
        let Acquire::Runner { epoch } = g.acquire() else {
            panic!("expected Runner");
        };
        // Bump the epoch (simulating a concurrent request).
        g.acquire(); // Recorded, epoch → 1
        match g.release(epoch) {
            Release::Pending(new_epoch) => assert_eq!(new_epoch, 1),
            Release::Idle => panic!("expected Pending"),
        }
        // Still running (the runner keeps the role on Pending).
        assert_eq!(g.packed.load(Ordering::Relaxed) & RUNNING, RUNNING);
    }

    #[test]
    fn multiple_bumps_then_release() {
        let g = GateState::new();
        let Acquire::Runner { epoch } = g.acquire() else {
            panic!("expected Runner");
        };
        assert_eq!(epoch, 0);
        // Three concurrent requests.
        g.acquire(); // epoch → 1
        g.acquire(); // epoch → 2
        g.acquire(); // epoch → 3
        match g.release(epoch) {
            Release::Pending(new) => assert_eq!(new, 3),
            Release::Idle => panic!("expected Pending"),
        }
        // Re-sync and release at the new epoch.
        match g.release(3) {
            Release::Idle => {}
            Release::Pending(_) => panic!("expected Idle"),
        }
        assert_eq!(g.packed.load(Ordering::Relaxed), 3 << 1); // idle, epoch 3
    }

    #[test]
    fn force_idle_clears_running_preserves_epoch() {
        let g = GateState::new();
        g.acquire(); // RUNNING, epoch 0
        g.acquire(); // bump → epoch 1
        g.force_idle();
        let s = g.packed.load(Ordering::Relaxed);
        assert_eq!(s & RUNNING, 0, "should be idle");
        assert_eq!(s >> 1, 1, "epoch should be preserved");
    }

    #[test]
    fn acquire_after_force_idle_gets_preserved_epoch() {
        let g = GateState::new();
        g.acquire(); // RUNNING, epoch 0
        g.acquire(); // bump → epoch 1
        g.force_idle();
        match g.acquire() {
            Acquire::Runner { epoch } => assert_eq!(epoch, 1),
            Acquire::Recorded => panic!("expected Runner"),
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    //! Loom verification of the [`GenerateWorkGate`] admission protocol.
    //!
    //! Invariant under test: no published work is ever stranded. Across every
    //! interleaving, a caller that publishes work and then loses the acquire race
    //! still has its work drained — the active runner makes an extra pass, or the
    //! completion edge re-acquires.
    //!
    //! Regimes covered:
    //! - **Coalescing** (`..._no_lost_wake`, `..._three_callers`): concurrent
    //!   callers with no capacity limit; the acquire-race loser is not stranded.
    //! - **At capacity** (`..._at_capacity`, `..._at_capacity_with_producer`): the
    //!   runner retires with work still queued, trusting an in-flight completion
    //!   to re-drive; the completion edge must pick it up.
    //! - **Bounded handoff and the `dispatched == 0` corner**
    //!   (`..._bounded_handoff`, `..._bounded_handoff_corner`,
    //!   `..._corner_unbounded`): a producer publishes while the last in-flight
    //!   item completes, leaving no completion edge. The epoch counts the two
    //!   concurrent requests rather than collapsing them, so the producer's is not
    //!   lost; `..._corner_unbounded` is the config a saturating 1-bit flag
    //!   strands on.
    //!
    //! Modeling: `dispatched` is `Relaxed` (matching the production counter),
    //! `ready` is `SeqCst` (standing in for the separately-synchronized ready
    //! set). No-lost-wake ordering therefore rests solely on the gate's `SeqCst`
    //! epoch CAS, the carrier production relies on.
    //!
    //! Regression check: breaking the protocol — `acquire` skipping the epoch bump
    //! on the active-runner path, or `release` ignoring `observed` and
    //! blind-clearing `RUNNING` — must make loom fail here. No broken-on-purpose
    //! copy is kept; it would be a second, diverging implementation of the
    //! protocol.

    use super::GenerateWorkGate;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Capacity bound for the at-capacity tests. Production's `target` is always
    /// `>= 1` (see `ConcurrencyController::target`); 1 is the tightest value and
    /// the one that maximizes retire-at-capacity races, so it is what we model.
    const TARGET: usize = 1;

    /// Unbounded pass budget — reproduces a runner that never declines a pass.
    const UNBOUNDED: usize = usize::MAX;

    /// Wraps the real gate with the counters needed to detect a stranded wake.
    /// `ready` is the queued-work count a caller publishes before driving the
    /// gate; a pass drains it into `processed`. `dispatched`/`max_seen` model
    /// in-flight work and assert capacity is never exceeded. `target`/`k` are the
    /// concurrency target and the proactive-pass bound (`MAX_GENERATION_PASSES`).
    struct GateHarness {
        gate: GenerateWorkGate,
        ready: AtomicUsize,
        processed: AtomicUsize,
        /// In-flight count — the model of `Scheduler::dispatched`. `Relaxed` to
        /// match production (`has_capacity` reads it `Relaxed`).
        dispatched: AtomicUsize,
        /// High-water mark of `dispatched`, to assert capacity is never exceeded.
        max_seen: AtomicUsize,
        target: usize,
        k: usize,
    }

    impl GateHarness {
        /// Target 1, unbounded passes — the shape the coalescing and at-capacity
        /// tests rely on.
        fn new() -> Self {
            Self::with(TARGET, UNBOUNDED)
        }

        /// Explicit `target` and proactive-pass bound `k`, for the
        /// bounded-handoff tests.
        fn with(target: usize, k: usize) -> Self {
            Self {
                gate: GenerateWorkGate::new(),
                ready: AtomicUsize::new(0),
                processed: AtomicUsize::new(0),
                dispatched: AtomicUsize::new(0),
                max_seen: AtomicUsize::new(0),
                target,
                k,
            }
        }

        /// Publish one work item, then run the production acquire/drain loop —
        /// the exact shape of `Scheduler::generate_work` with no capacity limit
        /// (every pass drains all of `ready`).
        fn insert_and_drive(&self) {
            self.ready.fetch_add(1, Ordering::SeqCst);
            let Some(mut permit) = self.gate.try_acquire() else {
                return;
            };
            loop {
                // One generation pass: drain everything currently queued.
                let n = self.ready.swap(0, Ordering::SeqCst);
                self.processed.fetch_add(n, Ordering::SeqCst);
                if permit.try_release() {
                    break;
                }
            }
        }

        /// `has_capacity()` — `dispatched < target`, the plain-load form used in
        /// the dispatch hot loop, modeled exactly as production.
        fn has_capacity(&self) -> bool {
            self.dispatched.load(Ordering::Relaxed) < self.target
        }

        /// `recheck_capacity()` — the retire-decision capacity check, reading
        /// `dispatched` with an RMW (`fetch_add(0, SeqCst)`) so it observes a
        /// racing completion's `fetch_sub` even when that completion's request
        /// was coalesced (epoch bumped) by another caller. A plain load here can
        /// read the stale pre-decrement value and retire into a lost wake;
        /// `..._with_producer` fails if this is reverted to `has_capacity`.
        /// Mirrors `Scheduler::recheck_capacity`.
        fn recheck_capacity(&self) -> bool {
            self.dispatched.fetch_add(0, Ordering::SeqCst) < self.target
        }

        /// One generation pass: dispatch queued work until capacity is hit or the
        /// ready set drains. Mirrors the inner `break_reason` loop of
        /// `generate_work` (pop a ready item, dispatch it, bump `dispatched`),
        /// collapsed to the capacity/dispatch interaction the gate composes with.
        fn run_pass(&self) {
            while self.has_capacity() && self.ready.load(Ordering::SeqCst) > 0 {
                self.ready.fetch_sub(1, Ordering::SeqCst);
                let n = self.dispatched.fetch_add(1, Ordering::Relaxed) + 1;
                self.processed.fetch_add(1, Ordering::SeqCst);
                self.max_seen.fetch_max(n, Ordering::Relaxed);
            }
        }

        /// Acquire the runner role and drain with the production bounded
        /// retire loop — the exact shape of the `Scheduler::generate_work` tail
        /// (including the `MAX_GENERATION_PASSES` bound, here `self.k`). After the
        /// bound, the runner declines fresh passes and hands off to the
        /// completion edge when one is guaranteed (`dispatched >= 1`); in the
        /// `dispatched == 0` corner it keeps draining (no edge exists).
        fn generate_work(&self) {
            let Some(mut permit) = self.gate.try_acquire() else {
                return;
            };
            let mut passes = 0usize;
            'generate: loop {
                self.run_pass();
                passes += 1;
                loop {
                    if permit.try_release() {
                        return;
                    }
                    // Request pending. RMW read (not has_capacity) so a coalesced
                    // completion's slot release is observed; see recheck_capacity.
                    if self.recheck_capacity() {
                        // Under the bound, or in the dispatched == 0 corner (no
                        // completion edge), run another pass. Otherwise retire and
                        // hand off to the guaranteed completion edge.
                        if passes < self.k || self.dispatched.fetch_add(0, Ordering::SeqCst) == 0 {
                            continue 'generate;
                        }
                    }
                    // At capacity or bound-handoff: retire and retry the CAS
                    // release rather than spin a pass that would dispatch nothing.
                }
            }
        }

        /// Free one in-flight slot then re-drive — the exact order of
        /// `Scheduler::on_completion` (`dispatched.fetch_sub`, then
        /// `generate_work`). This is the completion edge the
        /// retire-at-capacity path relies on to pick up stranded work.
        fn complete_one(&self) {
            self.dispatched.fetch_sub(1, Ordering::Relaxed);
            self.generate_work();
        }
    }

    /// Two callers each publish one item and drive the gate concurrently. The
    /// loser of the acquire race must not strand its item; loom explores every
    /// interleaving.
    #[test]
    fn generate_work_gate_no_lost_wake() {
        loom::model(|| {
            let h = Arc::new(GateHarness::new());

            let h1 = h.clone();
            let t1 = thread::spawn(move || h1.insert_and_drive());
            let h2 = h.clone();
            let t2 = thread::spawn(move || h2.insert_and_drive());
            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(
                h.ready.load(Ordering::SeqCst),
                0,
                "work stranded: a generate_work wake was lost"
            );
            assert_eq!(
                h.processed.load(Ordering::SeqCst),
                2,
                "both published items must be generated"
            );
        });
    }

    /// Three concurrent callers, each publishing one item. No work stranded.
    #[test]
    fn generate_work_gate_no_lost_wake_three_callers() {
        loom::model(|| {
            let h = Arc::new(GateHarness::new());

            let handles: Vec<_> = (0..3)
                .map(|_| {
                    let h = h.clone();
                    thread::spawn(move || h.insert_and_drive())
                })
                .collect();
            for t in handles {
                t.join().unwrap();
            }

            assert_eq!(
                h.ready.load(Ordering::SeqCst),
                0,
                "work stranded: a generate_work wake was lost"
            );
            assert_eq!(
                h.processed.load(Ordering::SeqCst),
                3,
                "all three published items must be generated"
            );
        });
    }

    /// Retire-at-capacity vs. a completion. Pre-seed one item in flight
    /// (`dispatched == TARGET == 1`) and one queued (`ready == 1`): the runner
    /// finds no capacity and must release, while a completion concurrently frees
    /// the slot and re-drives. Whatever the interleaving, the queued item must
    /// be dispatched — neither the runner's release nor the completion's bump
    /// may drop the other's request.
    #[test]
    fn generate_work_gate_no_lost_wake_at_capacity() {
        loom::model(|| {
            let h = Arc::new(GateHarness::new());
            // One item already in flight (at capacity), one item queued.
            h.dispatched.store(1, Ordering::Relaxed);
            h.max_seen.store(1, Ordering::Relaxed);
            h.ready.store(1, Ordering::SeqCst);

            let hr = h.clone();
            let runner = thread::spawn(move || hr.generate_work());
            let hc = h.clone();
            let completion = thread::spawn(move || hc.complete_one());
            runner.join().unwrap();
            completion.join().unwrap();

            // The queued item was dispatched (the completion freed the only slot,
            // so processed == 1) and nothing is stranded.
            assert_eq!(
                h.ready.load(Ordering::SeqCst),
                0,
                "work stranded at capacity: a completion-edge wake was lost"
            );
            assert_eq!(
                h.processed.load(Ordering::SeqCst),
                1,
                "the queued item must be dispatched once the slot frees"
            );
            // Capacity was never exceeded; the runner retired cleanly.
            assert_eq!(
                h.max_seen.load(Ordering::Relaxed),
                TARGET,
                "dispatched exceeded the concurrency target"
            );
        });
    }

    /// Same retire-at-capacity race, plus a third thread that publishes a new
    /// item and drives the gate (a `wake`/`enqueue` arriving mid-race, not just a
    /// completion).
    ///
    /// Accounting: `TARGET == 1` with one item already in flight means zero free
    /// capacity initially; the single completion frees exactly one slot, so
    /// exactly one of the two queued items (the pre-seeded one plus the
    /// producer's) can dispatch. The other is correctly held by backpressure —
    /// `dispatched == TARGET`, a future completion would re-drive it — NOT
    /// stranded. The wedge we guard against is the opposite: an item left queued
    /// with a FREE slot and no runner (`dispatched < TARGET`), which must never
    /// happen.
    #[test]
    fn generate_work_gate_no_lost_wake_at_capacity_with_producer() {
        loom::model(|| {
            let h = Arc::new(GateHarness::new());
            // One item already in flight (at capacity), one item queued.
            h.dispatched.store(1, Ordering::Relaxed);
            h.max_seen.store(1, Ordering::Relaxed);
            h.ready.store(1, Ordering::SeqCst);

            let hr = h.clone();
            let runner = thread::spawn(move || hr.generate_work());
            let hc = h.clone();
            let completion = thread::spawn(move || hc.complete_one());
            let hp = h.clone();
            let producer = thread::spawn(move || {
                hp.ready.fetch_add(1, Ordering::SeqCst);
                hp.generate_work();
            });
            runner.join().unwrap();
            completion.join().unwrap();
            producer.join().unwrap();

            let ready = h.ready.load(Ordering::SeqCst);
            let dispatched = h.dispatched.load(Ordering::Relaxed);
            let processed = h.processed.load(Ordering::SeqCst);
            let max_seen = h.max_seen.load(Ordering::Relaxed);
            // The only failure that matters: queued work with a FREE slot and no
            // runner left to drive it. That is a wedge — a lost wake.
            let wedge = ready > 0 && dispatched < TARGET;
            assert!(
                !wedge,
                "WEDGE: ready={ready} dispatched={dispatched} processed={processed} max_seen={max_seen}"
            );
            assert!(
                max_seen <= TARGET,
                "dispatched exceeded target: max_seen={max_seen} processed={processed}"
            );
        });
    }

    /// The corner that strands a saturating 1-bit gate: a producer publishes
    /// work while the last in-flight item completes (`dispatched -> 0`), runner
    /// at its pass bound. The epoch gate counts both requests, so the producer's
    /// is not lost.
    #[test]
    fn generate_work_gate_bounded_handoff_corner() {
        loom::model(|| {
            let h = Arc::new(GateHarness::with(3, 1));
            h.dispatched.store(1, Ordering::Relaxed);
            h.max_seen.store(1, Ordering::Relaxed);

            let hr = h.clone();
            let runner = thread::spawn(move || hr.generate_work());
            let hc = h.clone();
            let completion = thread::spawn(move || hc.complete_one());
            let hp = h.clone();
            let producer = thread::spawn(move || {
                hp.ready.fetch_add(1, Ordering::SeqCst);
                hp.generate_work();
            });
            runner.join().unwrap();
            completion.join().unwrap();
            producer.join().unwrap();

            let ready = h.ready.load(Ordering::SeqCst);
            let dispatched = h.dispatched.load(Ordering::Relaxed);
            let processed = h.processed.load(Ordering::SeqCst);
            let max_seen = h.max_seen.load(Ordering::Relaxed);
            let wedge = ready > 0 && dispatched == 0;
            assert!(
                !wedge,
                "WEDGE (epoch corner): ready={ready} dispatched={dispatched} processed={processed} max_seen={max_seen}"
            );
            assert!(
                max_seen <= 3,
                "dispatched exceeded target: max_seen={max_seen}"
            );
        });
    }

    /// Bounded handoff with capacity headroom: runner drains its one pass, a
    /// producer publishes a second item, a completion races. No strand, capacity
    /// respected.
    #[test]
    fn generate_work_gate_bounded_handoff() {
        loom::model(|| {
            let h = Arc::new(GateHarness::with(3, 1));
            h.dispatched.store(1, Ordering::Relaxed);
            h.max_seen.store(1, Ordering::Relaxed);
            h.ready.store(1, Ordering::SeqCst);

            let hr = h.clone();
            let runner = thread::spawn(move || hr.generate_work());
            let hc = h.clone();
            let completion = thread::spawn(move || hc.complete_one());
            let hp = h.clone();
            let producer = thread::spawn(move || {
                hp.ready.fetch_add(1, Ordering::SeqCst);
                hp.generate_work();
            });
            runner.join().unwrap();
            completion.join().unwrap();
            producer.join().unwrap();

            let ready = h.ready.load(Ordering::SeqCst);
            let dispatched = h.dispatched.load(Ordering::Relaxed);
            let processed = h.processed.load(Ordering::SeqCst);
            let max_seen = h.max_seen.load(Ordering::Relaxed);
            let wedge = ready > 0 && dispatched == 0;
            assert!(
                !wedge,
                "WEDGE (epoch handoff): ready={ready} dispatched={dispatched} processed={processed} max_seen={max_seen}"
            );
            assert!(
                max_seen <= 3,
                "dispatched exceeded target: max_seen={max_seen}"
            );
        });
    }

    /// The `dispatched == 0` corner with no pass bound (k = MAX). Isolates the
    /// epoch from the bounded-handoff guard: with no bound the runner keeps
    /// re-passing, so safety rests entirely on the release CAS observing the
    /// producer's epoch bump. A saturating 1-bit gate strands here.
    #[test]
    fn generate_work_gate_corner_unbounded() {
        loom::model(|| {
            let h = Arc::new(GateHarness::with(3, UNBOUNDED));
            h.dispatched.store(1, Ordering::Relaxed);
            h.max_seen.store(1, Ordering::Relaxed);

            let hr = h.clone();
            let runner = thread::spawn(move || hr.generate_work());
            let hc = h.clone();
            let completion = thread::spawn(move || hc.complete_one());
            let hp = h.clone();
            let producer = thread::spawn(move || {
                hp.ready.fetch_add(1, Ordering::SeqCst);
                hp.generate_work();
            });
            runner.join().unwrap();
            completion.join().unwrap();
            producer.join().unwrap();

            let ready = h.ready.load(Ordering::SeqCst);
            let dispatched = h.dispatched.load(Ordering::Relaxed);
            let processed = h.processed.load(Ordering::SeqCst);
            let max_seen = h.max_seen.load(Ordering::Relaxed);
            let wedge = ready > 0 && dispatched == 0;
            assert!(
                !wedge,
                "WEDGE (epoch corner unbounded): ready={ready} dispatched={dispatched} processed={processed} max_seen={max_seen}"
            );
            assert!(
                max_seen <= 3,
                "dispatched exceeded target: max_seen={max_seen}"
            );
        });
    }

    /// Two callers coalescing through the capacity-aware drive path
    /// (`generate_work`/`run_pass`/`recheck_capacity`, target and k high), rather
    /// than the `insert_and_drive` loop the other coalescing tests use. No work
    /// stranded.
    #[test]
    fn generate_work_gate_no_lost_wake_drive_path() {
        loom::model(|| {
            let h = Arc::new(GateHarness::with(usize::MAX, UNBOUNDED));

            let h1 = h.clone();
            let t1 = thread::spawn(move || {
                h1.ready.fetch_add(1, Ordering::SeqCst);
                h1.generate_work();
            });
            let h2 = h.clone();
            let t2 = thread::spawn(move || {
                h2.ready.fetch_add(1, Ordering::SeqCst);
                h2.generate_work();
            });
            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(
                h.ready.load(Ordering::SeqCst),
                0,
                "work stranded: a generate_work wake was lost"
            );
            assert_eq!(
                h.processed.load(Ordering::SeqCst),
                2,
                "both published items must be generated"
            );
        });
    }
}
