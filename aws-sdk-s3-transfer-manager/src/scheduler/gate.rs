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

/// No runner active and no request pending.
const IDLE: usize = 0;
/// A runner holds the permit and is draining generation passes.
const RUNNING: usize = 1;
/// A runner is active AND a request arrived while it ran; the runner must
/// drain once more before retiring.
const NOTIFIED: usize = 2;

/// Admits one work-generation runner at a time and coalesces concurrent
/// requests so none is lost.
///
/// Single-runner admission keeps ready-set pops sequential: a transfer gets the
/// chance to re-insert between pops, so priority ordering holds (parallel pops
/// would short-circuit the pop-poll-reinsert cycle priority depends on). A
/// caller that cannot acquire the permit records its request, and the active
/// runner drains an extra pass for it.
///
/// No-lost-wake guarantee: a notifier's `RUNNING → NOTIFIED` and the runner's
/// retiring `RUNNING → IDLE` are on the same atomic's modification order, so a
/// request cannot land between the runner choosing to retire and observing that
/// none is pending. Both must be `SeqCst`.
pub(in crate::scheduler) struct GenerateWorkGate {
    state: AtomicUsize,
}

impl GenerateWorkGate {
    pub(in crate::scheduler) fn new() -> Self {
        Self {
            state: AtomicUsize::new(IDLE),
        }
    }

    /// Try to become the sole work-generation runner.
    ///
    /// Returns `Some(permit)` if this caller acquired the runner role and
    /// must drive generation passes until [`GenerateWorkPermit::try_release`]
    /// reports the permit released. Returns `None` if a runner is already
    /// active — the call has recorded a request so that runner drains at
    /// least one more pass, so the caller's just-published work is not
    /// stranded.
    ///
    /// Callers MUST publish their work (insert into the ready set) BEFORE
    /// calling this; otherwise the extra pass may run before the work is
    /// visible. `None` is also the re-entrancy guard: a re-entrant call on the
    /// runner's own thread records the request and returns rather than
    /// recursing into a nested pass.
    pub(in crate::scheduler) fn try_acquire(&self) -> Option<GenerateWorkPermit<'_>> {
        loop {
            match self.state.compare_exchange_weak(
                IDLE,
                RUNNING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // Won the runner role.
                Ok(_) => {
                    return Some(GenerateWorkPermit {
                        gate: self,
                        released: false,
                    })
                }
                // A runner is active: record a re-drain request for it.
                Err(RUNNING) => {
                    if self
                        .state
                        .compare_exchange_weak(
                            RUNNING,
                            NOTIFIED,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return None;
                    }
                    // Lost to a concurrent transition; re-observe and retry.
                }
                // A re-drain is already pending; the runner covers us.
                Err(NOTIFIED) => return None,
                // Transient CAS failure (the weak form may spuriously fail,
                // or the state changed under us); re-observe and retry.
                Err(_) => {}
            }
        }
    }

    /// Retire the runner role, or re-arm it if a request arrived. Called by
    /// the permit; see [`GenerateWorkPermit::try_release`].
    fn finish_pass(&self) -> bool {
        match self
            .state
            .compare_exchange(RUNNING, IDLE, Ordering::SeqCst, Ordering::SeqCst)
        {
            // No request pending: retired.
            Ok(_) => true,
            // A request arrived during the pass: re-arm and run again.
            Err(NOTIFIED) => {
                self.state.store(RUNNING, Ordering::SeqCst);
                false
            }
            Err(_) => unreachable!("only the runner transitions out of RUNNING"),
        }
    }

    /// Reset to `IDLE` on a panic unwinding out of a pass. See
    /// [`GenerateWorkPermit`]'s `Drop`.
    fn force_idle(&self) {
        self.state.store(IDLE, Ordering::SeqCst);
    }
}

/// Proof that the holder is the sole active work-generation runner.
///
/// Held across the runner's whole drain loop — many passes, not one — and
/// relinquished by [`try_release`](Self::try_release) returning `true`,
/// unlike a semaphore permit that releases on drop. `Drop` is the
/// panic-safety fallback only: if a pass unwinds while the permit is still
/// held, it resets the gate so generation can resume (otherwise the runner
/// role stays `RUNNING` and every future caller bails forever, wedging the
/// scheduler). A clean `try_release` disarms the drop.
pub(in crate::scheduler) struct GenerateWorkPermit<'a> {
    gate: &'a GenerateWorkGate,
    /// Set once `try_release` reports the permit released; suppresses the
    /// `Drop` reset so it cannot stomp a runner that has since acquired.
    released: bool,
}

impl GenerateWorkPermit<'_> {
    /// Call after each generation pass. Returns `true` if the permit was
    /// released — no request pending, stop draining. Returns `false` if a
    /// request arrived during the pass — keep the permit and run another
    /// pass. Takes `&mut self` because on `false` the permit is retained.
    pub(in crate::scheduler) fn try_release(&mut self) -> bool {
        if self.gate.finish_pass() {
            self.released = true;
            true
        } else {
            false
        }
    }
}

impl Drop for GenerateWorkPermit<'_> {
    fn drop(&mut self) {
        // A clean release already retired the role; only reset here on an
        // unwind, so a panicking pass does not wedge generation at RUNNING.
        if !self.released {
            self.gate.force_idle();
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    //! Loom verification of the [`GenerateWorkGate`] admission protocol.
    //!
    //! These drive the production gate directly. The only test-supplied piece
    //! is the generation pass: a closure draining a `ready` counter (the
    //! queued-work count) into `processed`. The invariant: across every
    //! interleaving, no published work is left in `ready` once all callers
    //! return — a stranded item means a wake was lost.
    //!
    //! To confirm these still catch a regression, mutate the gate to break the
    //! protocol (e.g. have `try_acquire` bail on `RUNNING` without recording the
    //! request) and check loom fails here; revert once confirmed. A permanent
    //! broken-on-purpose copy is not kept — it would be a second, diverging
    //! implementation of the protocol.

    use super::GenerateWorkGate;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Wraps the real gate with the counters needed to detect a stranded wake.
    /// `ready` is the queued-work count a caller publishes before driving the
    /// gate; a pass drains it into `processed`.
    struct GateHarness {
        gate: GenerateWorkGate,
        ready: AtomicUsize,
        processed: AtomicUsize,
    }

    impl GateHarness {
        fn new() -> Self {
            Self {
                gate: GenerateWorkGate::new(),
                ready: AtomicUsize::new(0),
                processed: AtomicUsize::new(0),
            }
        }

        /// Publish one work item, then run the production acquire/drain loop —
        /// the exact shape of `Scheduler::generate_work`.
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

    /// Three concurrent callers: widens the interleaving space so a
    /// `RUNNING → NOTIFIED → RUNNING` re-arm overlapping a third request is
    /// exercised. No work may be stranded.
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
}
