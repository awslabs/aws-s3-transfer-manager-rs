/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Lock-free aggregate acquisition accounting.
//!
//! Active planned demand remains under admission serialization. Available
//! coverage and uncovered charges share one atomic word so debit, return,
//! grant, and close cannot observe or publish a torn pair.

use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering};

use super::super::{invariant_violation, CarrierCount};

/// Number of bits in one packed accounting lane.
const LANE_BITS: u32 = u32::BITS;

/// Largest count represented by one packed accounting lane.
pub(in crate::runtime::buffer_pool) const MAX_PACKED_CARRIERS: CarrierCount =
    CarrierCount::new(u32::MAX as usize);

/// A packed transition cannot represent its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct CoverageOverflow;

/// Packed coverage and ownership pressure updated by acquisition and return.
pub(in crate::runtime::buffer_pool) struct CoverageState {
    /// Low lane: available coverage. High lane: uncovered charges.
    packed: AtomicU64,
}

/// One coherent sample of [`CoverageState`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct CoverageSnapshot {
    /// Open-envelope capacity not occupied by an acquisition.
    pub(in crate::runtime::buffer_pool) available: CarrierCount,
    /// Ownership pressure outside open-envelope coverage.
    pub(in crate::runtime::buffer_pool) uncovered: CarrierCount,
}

/// Result of installing aggregate acquisition charges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct CoverageDebit {
    /// Uncovered charges introduced by this transition.
    pub(in crate::runtime::buffer_pool) uncovered_added: CarrierCount,
}

/// Result of retiring aggregate acquisition charges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct CoverageReturn {
    /// Uncovered charges removed by this transition.
    pub(in crate::runtime::buffer_pool) uncovered_removed: CarrierCount,
}

impl CoverageState {
    /// Creates empty aggregate accounting.
    pub(in crate::runtime::buffer_pool) fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }

    /// Debits only when available coverage satisfies the complete request.
    ///
    /// `Ok(false)` leaves state unchanged. The caller must enter admission
    /// serialization before installing an uncovered charge.
    pub(in crate::runtime::buffer_pool) fn try_debit_covered(
        &self,
        count: CarrierCount,
    ) -> Result<bool, CoverageOverflow> {
        checked_lane(count)?;
        let mut current = self.packed.load(Ordering::Acquire);

        loop {
            let snapshot = unpack(current);
            let Some(available) = snapshot.available.checked_sub(count) else {
                return Ok(false);
            };
            let next = pack(available, snapshot.uncovered)?;
            match self.packed.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(true),
                Err(observed) => current = observed,
            }
        }
    }

    /// Installs acquisition charges, consuming coverage before adding overage.
    ///
    /// Callers that may add an uncovered charge hold admission serialization.
    /// `maximum_uncovered` enforces the packed global admission bound.
    pub(in crate::runtime::buffer_pool) fn debit(
        &self,
        count: CarrierCount,
        maximum_uncovered: CarrierCount,
    ) -> Result<CoverageDebit, CoverageOverflow> {
        checked_lane(count)?;
        checked_lane(maximum_uncovered)?;
        self.update(|snapshot| {
            let covered = CarrierCount::new(count.get().min(snapshot.available.get()));
            let uncovered_added = count.checked_sub(covered).ok_or(CoverageOverflow)?;
            let available = snapshot
                .available
                .checked_sub(covered)
                .ok_or(CoverageOverflow)?;
            let uncovered = snapshot
                .uncovered
                .checked_add(uncovered_added)
                .filter(|next| *next <= maximum_uncovered)
                .ok_or(CoverageOverflow)?;
            Ok((
                CoverageSnapshot {
                    available,
                    uncovered,
                },
                CoverageDebit { uncovered_added },
            ))
        })
    }

    /// Retires charges, removing uncovered charges before restoring coverage.
    pub(in crate::runtime::buffer_pool) fn release(&self, count: CarrierCount) -> CoverageReturn {
        if checked_lane(count).is_err() {
            invariant_violation("released carrier count exceeds packed accounting");
        }
        self.update(|snapshot| {
            let uncovered_removed = CarrierCount::new(count.get().min(snapshot.uncovered.get()));
            let restored = count
                .checked_sub(uncovered_removed)
                .ok_or(CoverageOverflow)?;
            let available = snapshot
                .available
                .checked_add(restored)
                .ok_or(CoverageOverflow)?;
            let uncovered = snapshot
                .uncovered
                .checked_sub(uncovered_removed)
                .ok_or(CoverageOverflow)?;
            Ok((
                CoverageSnapshot {
                    available,
                    uncovered,
                },
                CoverageReturn { uncovered_removed },
            ))
        })
        .unwrap_or_else(|_| invariant_violation("aggregate return exceeds packed accounting"))
    }

    /// Publishes unused coverage from one newly active envelope.
    ///
    /// Existing uncovered charges remain unchanged. A later grant cannot
    /// absorb ownership that predates it.
    pub(in crate::runtime::buffer_pool) fn add_coverage(
        &self,
        count: CarrierCount,
    ) -> Result<(), CoverageOverflow> {
        checked_lane(count)?;
        self.update(|snapshot| {
            let available = snapshot
                .available
                .checked_add(count)
                .ok_or(CoverageOverflow)?;
            Ok((
                CoverageSnapshot {
                    available,
                    uncovered: snapshot.uncovered,
                },
                (),
            ))
        })
    }

    /// Withdraws one closing envelope without consuming another envelope's coverage.
    ///
    /// Direct outstanding charges are already present in aggregate accounting.
    /// Availability above remaining active demand belongs to a return that
    /// raced the direct snapshot and must also leave with this envelope.
    pub(in crate::runtime::buffer_pool) fn remove_coverage(
        &self,
        envelope: CarrierCount,
        direct_outstanding: CarrierCount,
        remaining_active: CarrierCount,
    ) {
        if checked_lane(envelope).is_err() {
            invariant_violation("closed envelope exceeds packed accounting");
        }
        if checked_lane(direct_outstanding).is_err() {
            invariant_violation("closing direct outstanding exceeds packed accounting");
        }
        if checked_lane(remaining_active).is_err() {
            invariant_violation("remaining active demand exceeds packed accounting");
        }
        let potentially_unused = envelope.checked_sub(direct_outstanding).unwrap_or_else(|| {
            invariant_violation("closing direct outstanding exceeds its envelope")
        });
        self.update(|snapshot| {
            let nominally_unused =
                CarrierCount::new(potentially_unused.get().min(snapshot.available.get()));
            let required_for_active = CarrierCount::new(
                snapshot
                    .available
                    .get()
                    .saturating_sub(remaining_active.get()),
            );
            let removed = nominally_unused.max(required_for_active);
            let reclassified = envelope.checked_sub(removed).ok_or(CoverageOverflow)?;
            let available = snapshot
                .available
                .checked_sub(removed)
                .ok_or(CoverageOverflow)?;
            let uncovered = snapshot
                .uncovered
                .checked_add(reclassified)
                .ok_or(CoverageOverflow)?;
            Ok((
                CoverageSnapshot {
                    available,
                    uncovered,
                },
                (),
            ))
        })
        .unwrap_or_else(|_| invariant_violation("reservation close exceeds packed accounting"));
    }

    /// Loads one coherent accounting sample.
    pub(in crate::runtime::buffer_pool) fn snapshot(&self) -> CoverageSnapshot {
        unpack(self.packed.load(Ordering::Acquire))
    }

    fn update<T>(
        &self,
        mut transition: impl FnMut(CoverageSnapshot) -> Result<(CoverageSnapshot, T), CoverageOverflow>,
    ) -> Result<T, CoverageOverflow> {
        let mut current = self.packed.load(Ordering::Acquire);
        loop {
            let (next, result) = transition(unpack(current))?;
            let next = pack(next.available, next.uncovered)?;
            match self.packed.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(result),
                Err(observed) => current = observed,
            }
        }
    }
}

fn checked_lane(count: CarrierCount) -> Result<u32, CoverageOverflow> {
    count.try_as_u32().ok_or(CoverageOverflow)
}

fn pack(available: CarrierCount, uncovered: CarrierCount) -> Result<u64, CoverageOverflow> {
    let available = checked_lane(available)?;
    let uncovered = checked_lane(uncovered)?;
    Ok(u64::from(available) | (u64::from(uncovered) << LANE_BITS))
}

fn unpack(packed: u64) -> CoverageSnapshot {
    CoverageSnapshot {
        available: CarrierCount::new((packed as u32) as usize),
        uncovered: CarrierCount::new((packed >> LANE_BITS) as usize),
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::*;

    fn state_with(available: usize, uncovered: usize) -> CoverageState {
        CoverageState {
            packed: AtomicU64::new(
                pack(CarrierCount::new(available), CarrierCount::new(uncovered)).unwrap(),
            ),
        }
    }

    #[test]
    fn test_debit_consumes_coverage_before_adding_uncovered_charge() {
        let state = state_with(3, 0);

        let debit = state
            .debit(CarrierCount::new(5), CarrierCount::new(8))
            .unwrap();

        assert_eq!(debit.uncovered_added, CarrierCount::new(2));
        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::new(2),
            }
        );
    }

    #[test]
    fn test_covered_debit_miss_leaves_state_unchanged() {
        let state = state_with(1, 2);
        let before = state.snapshot();

        assert!(!state.try_debit_covered(CarrierCount::new(2)).unwrap());
        assert_eq!(state.snapshot(), before);
    }

    #[test]
    fn test_return_removes_uncovered_before_restoring_coverage() {
        let state = state_with(0, 3);

        let returned = state.release(CarrierCount::new(5));

        assert_eq!(returned.uncovered_removed, CarrierCount::new(3));
        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::new(2),
                uncovered: CarrierCount::ZERO,
            }
        );
    }

    #[test]
    fn test_new_envelope_does_not_absorb_existing_uncovered_charge() {
        let state = state_with(0, 1);

        state.add_coverage(CarrierCount::new(1)).unwrap();

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::new(1),
                uncovered: CarrierCount::new(1),
            }
        );
    }

    #[test]
    fn test_close_reclassifies_occupied_coverage() {
        let state = state_with(4, 0);
        assert!(state.try_debit_covered(CarrierCount::new(3)).unwrap());

        state.remove_coverage(
            CarrierCount::new(4),
            CarrierCount::new(3),
            CarrierCount::ZERO,
        );

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::new(3),
            }
        );
    }

    #[test]
    fn test_close_preserves_coverage_needed_by_other_direct_authority() {
        let state = state_with(3, 0);

        state.remove_coverage(
            CarrierCount::new(3),
            CarrierCount::new(3),
            CarrierCount::new(3),
        );

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::new(3),
                uncovered: CarrierCount::new(3),
            }
        );
    }

    #[test]
    fn test_close_reclassifies_nominally_unused_coverage_consumed_by_unreserved_acquisition() {
        let state = state_with(1, 0);

        state.remove_coverage(CarrierCount::new(3), CarrierCount::ZERO, CarrierCount::ZERO);

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::new(2),
            }
        );
    }

    #[test]
    fn test_small_debit_space_matches_transition_equations() {
        for available in 0..=4 {
            for uncovered in 0..=4 {
                for count in 0..=6 {
                    let state = state_with(available, uncovered);
                    let debit = state
                        .debit(CarrierCount::new(count), CarrierCount::new(10))
                        .unwrap();
                    let covered = count.min(available);

                    assert_eq!(debit.uncovered_added, CarrierCount::new(count - covered));
                    assert_eq!(
                        state.snapshot(),
                        CoverageSnapshot {
                            available: CarrierCount::new(available - covered),
                            uncovered: CarrierCount::new(uncovered + count - covered),
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn test_small_return_space_matches_transition_equations() {
        for available in 0..=4 {
            for uncovered in 0..=4 {
                for count in 0..=4 {
                    let state = state_with(available, uncovered);
                    let returned = state.release(CarrierCount::new(count));
                    let uncovered_removed = count.min(uncovered);

                    assert_eq!(
                        returned.uncovered_removed,
                        CarrierCount::new(uncovered_removed)
                    );
                    assert_eq!(
                        state.snapshot(),
                        CoverageSnapshot {
                            available: CarrierCount::new(available + count - uncovered_removed),
                            uncovered: CarrierCount::new(uncovered - uncovered_removed),
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn test_small_close_space_matches_transition_equations() {
        for available in 0..=4 {
            for uncovered in 0..=4 {
                for envelope in 0..=6 {
                    for direct_outstanding in 0..=envelope {
                        for remaining_active in 0..=4 {
                            if available + direct_outstanding > remaining_active + envelope {
                                continue;
                            }
                            let state = state_with(available, uncovered);
                            state.remove_coverage(
                                CarrierCount::new(envelope),
                                CarrierCount::new(direct_outstanding),
                                CarrierCount::new(remaining_active),
                            );
                            let potentially_unused = envelope - direct_outstanding;
                            let nominally_unused = potentially_unused.min(available);
                            let required_for_active = available.saturating_sub(remaining_active);
                            let removed = nominally_unused.max(required_for_active);

                            assert_eq!(
                                state.snapshot(),
                                CoverageSnapshot {
                                    available: CarrierCount::new(available - removed),
                                    uncovered: CarrierCount::new(uncovered + envelope - removed),
                                }
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_packed_lane_boundary_is_checked() {
        let state = state_with(u32::MAX as usize, 0);

        assert_eq!(
            state.add_coverage(CarrierCount::new(1)),
            Err(CoverageOverflow)
        );
        if let Ok(too_large) = usize::try_from(u64::from(u32::MAX) + 1) {
            assert_eq!(
                state.try_debit_covered(CarrierCount::new(too_large)),
                Err(CoverageOverflow)
            );
        }
        assert_eq!(
            state.snapshot().available,
            CarrierCount::new(u32::MAX as usize)
        );
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

    use super::*;

    #[test]
    fn test_debit_racing_close_preserves_one_live_charge() {
        loom::model(|| {
            let state = Arc::new(CoverageState::new());
            state.add_coverage(CarrierCount::new(1)).unwrap();

            let debiting = Arc::clone(&state);
            let debit = thread::spawn(move || {
                if !debiting.try_debit_covered(CarrierCount::new(1)).unwrap() {
                    debiting
                        .debit(CarrierCount::new(1), MAX_PACKED_CARRIERS)
                        .unwrap();
                }
            });
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || {
                closing.remove_coverage(
                    CarrierCount::new(1),
                    CarrierCount::ZERO,
                    CarrierCount::ZERO,
                )
            });

            debit.join().unwrap();
            close.join().unwrap();
            assert_eq!(
                state.snapshot(),
                CoverageSnapshot {
                    available: CarrierCount::ZERO,
                    uncovered: CarrierCount::new(1),
                }
            );
        });
    }

    #[test]
    fn test_return_racing_close_retires_live_charge() {
        loom::model(|| {
            let state = Arc::new(CoverageState::new());
            state.add_coverage(CarrierCount::new(1)).unwrap();
            assert!(state.try_debit_covered(CarrierCount::new(1)).unwrap());

            let returning = Arc::clone(&state);
            let release = thread::spawn(move || returning.release(CarrierCount::new(1)));
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || {
                closing.remove_coverage(
                    CarrierCount::new(1),
                    CarrierCount::new(1),
                    CarrierCount::ZERO,
                )
            });

            release.join().unwrap();
            close.join().unwrap();
            assert_eq!(
                state.snapshot(),
                CoverageSnapshot {
                    available: CarrierCount::ZERO,
                    uncovered: CarrierCount::ZERO,
                }
            );
        });
    }
}
