/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Lock-free aggregate acquisition accounting.
//!
//! Active planned demand remains protected by the admission mutex. This module
//! packs available coverage and sticky debt into one atomic word so acquisition,
//! return, grant, and close cannot observe a torn pair.

use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering};

/// Maximum instantaneous count represented by one packed field.
pub(super) const MAX_CARRIERS: usize = u32::MAX as usize;

const DEBT_SHIFT: u32 = u32::BITS;

/// A packed counter cannot represent the requested transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CountOverflow;

/// Aggregate state changed at carrier acquisition frequency.
pub(in crate::runtime::buffer_pool) struct CoverageState {
    packed: AtomicU64,
}

/// One coherent sample of [`CoverageState`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct CoverageSnapshot {
    /// Active planned demand not occupied by an acquisition.
    pub(in crate::runtime::buffer_pool) available_coverage: usize,
    /// Ownership pressure not covered by active planned demand.
    pub(in crate::runtime::buffer_pool) debt: usize,
}

/// Result of installing aggregate acquisition charges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Debit {
    /// Additional debt introduced by this transition.
    pub(super) new_debt: usize,
}

/// Result of retiring aggregate acquisition charges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Release {
    /// Debt removed by this transition.
    pub(super) repaid_debt: usize,
}

impl CoverageState {
    /// Create empty aggregate state.
    pub(in crate::runtime::buffer_pool) fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }

    /// Consume coverage without entering admission serialization.
    ///
    /// A false result leaves the state unchanged. The caller must enter the
    /// serialized path, which may install debt and prepare capacity.
    pub(super) fn try_debit_covered(&self, count: usize) -> Result<bool, CountOverflow> {
        let count = checked_count(count)?;
        let mut current = self.packed.load(Ordering::Acquire);

        loop {
            let snapshot = unpack(current);
            if snapshot.available_coverage < count {
                return Ok(false);
            }
            let next = pack(snapshot.available_coverage - count, snapshot.debt);
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

    /// Install acquisition charges, consuming coverage before creating debt.
    ///
    /// Debt-producing callers hold admission serialization so preparation and
    /// admission pressure remain one logical transaction.
    pub(super) fn debit(&self, count: usize, maximum_debt: usize) -> Result<Debit, CountOverflow> {
        let count = checked_count(count)?;
        let maximum_debt = checked_count(maximum_debt)?;
        self.update(|snapshot| {
            let covered = count.min(snapshot.available_coverage);
            let new_debt = count - covered;
            let debt = snapshot.debt.checked_add(new_debt).ok_or(CountOverflow)?;
            if debt > maximum_debt {
                return Err(CountOverflow);
            }
            Ok((
                CoverageSnapshot {
                    available_coverage: snapshot.available_coverage - covered,
                    debt,
                },
                Debit { new_debt },
            ))
        })
    }

    /// Retire acquisition charges, repaying debt before restoring coverage.
    pub(super) fn release(&self, count: usize) -> Release {
        let count = checked_count(count).expect("released carrier count exceeds packed state");
        self.update(|snapshot| {
            let repaid_debt = count.min(snapshot.debt);
            let restored_coverage = count - repaid_debt;
            let available_coverage = snapshot
                .available_coverage
                .checked_add(restored_coverage)
                .expect("aggregate return exceeds live ownership");
            Ok((
                CoverageSnapshot {
                    available_coverage,
                    debt: snapshot.debt - repaid_debt,
                },
                Release { repaid_debt },
            ))
        })
        .expect("aggregate return must fit packed state")
    }

    /// Publish coverage from a newly active reservation.
    ///
    /// Existing debt remains sticky. New coverage is available only to future
    /// acquisition.
    pub(super) fn add_coverage(&self, count: usize) -> Result<(), CountOverflow> {
        let count = checked_count(count)?;
        self.update(|snapshot| {
            let available_coverage = snapshot
                .available_coverage
                .checked_add(count)
                .ok_or(CountOverflow)?;
            Ok((
                CoverageSnapshot {
                    available_coverage,
                    debt: snapshot.debt,
                },
                (),
            ))
        })
    }

    /// Withdraw coverage from a closing reservation.
    ///
    /// Unused coverage is removed first. Any remainder becomes debt so
    /// outstanding acquisitions remain charged.
    pub(super) fn remove_coverage(&self, count: usize) {
        let count = checked_count(count).expect("removed coverage exceeds packed state");
        self.update(|snapshot| {
            let removed = count.min(snapshot.available_coverage);
            let new_debt = count - removed;
            let debt = snapshot
                .debt
                .checked_add(new_debt)
                .expect("closing coverage overflowed unreserved debt");
            Ok((
                CoverageSnapshot {
                    available_coverage: snapshot.available_coverage - removed,
                    debt,
                },
                (),
            ))
        })
        .expect("closing coverage must fit packed state");
    }

    /// Load one coherent diagnostic and admission sample.
    pub(in crate::runtime::buffer_pool) fn snapshot(&self) -> CoverageSnapshot {
        unpack(self.packed.load(Ordering::Acquire))
    }

    fn update<T>(
        &self,
        mut transition: impl FnMut(CoverageSnapshot) -> Result<(CoverageSnapshot, T), CountOverflow>,
    ) -> Result<T, CountOverflow> {
        let mut current = self.packed.load(Ordering::Acquire);
        loop {
            let (next, result) = transition(unpack(current))?;
            match self.packed.compare_exchange_weak(
                current,
                pack(next.available_coverage, next.debt),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(result),
                Err(observed) => current = observed,
            }
        }
    }
}

fn checked_count(count: usize) -> Result<usize, CountOverflow> {
    if count <= MAX_CARRIERS {
        Ok(count)
    } else {
        Err(CountOverflow)
    }
}

fn pack(available_coverage: usize, debt: usize) -> u64 {
    let available_coverage =
        u32::try_from(available_coverage).expect("available coverage exceeds packed state");
    let debt = u32::try_from(debt).expect("aggregate debt exceeds packed state");
    u64::from(available_coverage) | (u64::from(debt) << DEBT_SHIFT)
}

fn unpack(packed: u64) -> CoverageSnapshot {
    CoverageSnapshot {
        available_coverage: (packed as u32) as usize,
        debt: (packed >> DEBT_SHIFT) as usize,
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::*;

    #[test]
    fn grant_does_not_absorb_existing_debt() {
        let state = CoverageState::new();
        assert_eq!(state.debit(1, MAX_CARRIERS).unwrap().new_debt, 1);
        state.add_coverage(1).unwrap();

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available_coverage: 1,
                debt: 1,
            }
        );
    }

    #[test]
    fn close_converts_consumed_coverage_to_debt() {
        let state = CoverageState::new();
        state.add_coverage(1).unwrap();
        assert!(state.try_debit_covered(1).unwrap());
        state.remove_coverage(1);

        assert_eq!(
            state.snapshot(),
            CoverageSnapshot {
                available_coverage: 0,
                debt: 1,
            }
        );
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

    use super::*;

    #[test]
    fn acquire_racing_close_preserves_live_charge() {
        loom::model(|| {
            let state = Arc::new(CoverageState::new());
            state.add_coverage(1).unwrap();

            let acquiring = Arc::clone(&state);
            let acquire = thread::spawn(move || {
                if !acquiring.try_debit_covered(1).unwrap() {
                    acquiring.debit(1, MAX_CARRIERS).unwrap();
                }
            });
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || closing.remove_coverage(1));

            acquire.join().unwrap();
            close.join().unwrap();
            assert_eq!(
                state.snapshot(),
                CoverageSnapshot {
                    available_coverage: 0,
                    debt: 1,
                }
            );
        });
    }

    #[test]
    fn return_racing_close_retires_live_charge() {
        loom::model(|| {
            let state = Arc::new(CoverageState::new());
            state.add_coverage(1).unwrap();
            assert!(state.try_debit_covered(1).unwrap());

            let returning = Arc::clone(&state);
            let release = thread::spawn(move || returning.release(1));
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || closing.remove_coverage(1));

            release.join().unwrap();
            close.join().unwrap();
            assert_eq!(
                state.snapshot(),
                CoverageSnapshot {
                    available_coverage: 0,
                    debt: 0,
                }
            );
        });
    }
}
